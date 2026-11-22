// Runtime transcript probe: replays the recorded Codex fixture `provider_thread_resume` through the
// orchestrator (the same path as testkit/OrchestratorReplayRecovery.integration.test.ts) and reports
// what the second runtime's restored usage report costs the app-server read loop.
//
// Copied into apps/server/src/orchestration-v2/testkit/ by run.sh, run once, then removed. Not part of
// the change. Counts come from a scheduler that wraps Effect's default MixedScheduler:
//   tasks       = fiber tasks scheduled (one per Effect.yieldNow, fork or async resume)
//   dispatches  = setImmediate calls the scheduler made (event-loop iterations it asked for)
// Wall times are measured with performance.now() and are labelled as such; they vary with load.
// PS011_LOAD_MS=<n> adds a synthetic busy loop: while the second runtime starts its turn, every
// event-loop iteration also runs an <n> ms busy callback, as on a server busy with other work.
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { CodexSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  materializeReplayTranscriptWorkspace,
  readProviderReplayTranscript,
} from "@t3tools/provider-testing/replayTranscript";
import { appendFileSync } from "node:fs";

import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import * as CodexAdapterV2 from "../Adapters/CodexAdapterV2.ts";
import {
  CodexOrchestratorReplayHarness,
  withCodexReplayChildMetadata,
} from "../Adapters/CodexAdapterV2.testkit.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import {
  CODEX_MODEL_SELECTION,
  materializeFixtureInput,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "./fixtures/shared.ts";
import {
  makeReplayServerConfig,
  runOrchestratorV2ProviderReplayScenario,
} from "./ProviderReplayHarness.ts";

const OUT = process.env.PS011_OUT ?? "";
const LABEL = process.env.PS011_LABEL ?? "tree";
const LOAD_MS = Number(process.env.PS011_LOAD_MS ?? "0");
const say = (line: string) => {
  console.log(line);
  if (OUT) appendFileSync(OUT, `${line}\n`);
};

let tasks = 0;
let dispatches = 0;
const base = new Scheduler.MixedScheduler("async", (callback) => {
  dispatches += 1;
  const handle = setImmediate(callback);
  return () => clearImmediate(handle);
});
const countingScheduler: Scheduler.Scheduler = {
  executionMode: base.executionMode,
  shouldYield: (fiber) => base.shouldYield(fiber),
  makeDispatcher: () => {
    const dispatcher = base.makeDispatcher();
    return {
      scheduleTask: (task, priority) => {
        tasks += 1;
        dispatcher.scheduleTask(task, priority);
      },
      flush: () => dispatcher.flush(),
    };
  },
};

interface Mark {
  readonly phase: "start" | "end";
  readonly runtime: number;
  readonly method: string;
  readonly turnId: string | undefined;
  readonly tasks: number;
  readonly dispatches: number;
  readonly ms: number;
}
const marks: Array<Mark> = [];
let runtimes = 0;
let ticking = false;
let ticks = 0;
const tick = () => {
  if (!ticking) return;
  ticks += 1;
  const until = performance.now() + LOAD_MS;
  while (performance.now() < until) {
    // busy: simulated work from other threads on the same server
  }
  setImmediate(tick);
};

const turnIdOf = (method: string, payload: unknown): string | undefined => {
  const params = payload as { turnId?: unknown; turn?: { id?: unknown } } | null;
  if (method === "turn/started" || method === "turn/completed") {
    return typeof params?.turn?.id === "string" ? params.turn.id : undefined;
  }
  return typeof params?.turnId === "string" ? params.turnId : undefined;
};

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});

// Same as CodexAdapterV2.testkit.ts `layer`, with every notification handler wrapped to mark when the
// read loop enters and leaves it.
function probeLayer(input: {
  readonly transcript: CodexReplay.CodexAppServerReplayTranscript;
  readonly driver: CodexReplay.CodexAppServerReplayDriver;
}) {
  const layerReplay = CodexReplay.layerReplayWithDriver(input.driver);
  const layerReplayClientFactory = Layer.succeed(CodexAdapterV2.CodexAppServerClientFactory, {
    open: (openInput) =>
      Effect.gen(function* () {
        const context = yield* Layer.build(layerReplay).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                providerSessionId: openInput.providerSessionId,
                cause,
              }),
          ),
        );
        runtimes += 1;
        const runtime = runtimes;
        if (runtime === 2 && LOAD_MS > 0) {
          ticking = true;
          setImmediate(tick);
        }
        return yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
          Effect.map((client) => withCodexReplayChildMetadata(client, input.transcript)),
          Effect.map(
            (client) =>
              ({
                ...client,
                handleServerNotification: (method, handler) =>
                  client.handleServerNotification(method, (payload) =>
                    Effect.suspend(() => {
                      const turnId = turnIdOf(method, payload);
                      const mark = (phase: "start" | "end") =>
                        marks.push({
                          phase,
                          runtime,
                          method,
                          turnId,
                          tasks,
                          dispatches,
                          ms: performance.now(),
                        });
                      mark("start");
                      if (runtime === 2 && method === "turn/started") ticking = false;
                      return handler(payload).pipe(Effect.ensuring(Effect.sync(() => mark("end"))));
                    }),
                  ),
              }) as typeof client,
          ),
          Effect.provide(context),
        );
      }),
  });
  const layerServerConfig = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(input.transcript.scenario).pipe(Effect.orDie),
  ).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    CodexAdapterV2.createCodexAdapterV2({
      instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
      displayName: undefined,
      environment: [],
      enabled: true,
      config: DEFAULT_CODEX_SETTINGS,
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        layerReplayClientFactory,
        layerServerConfig,
        NodeServices.layer,
        NodeCrypto.layer,
        IdAllocator.layer,
        PortScanner.layerTest,
      ),
    ),
  );
}

const decodeCodexTranscript = Schema.decodeUnknownEffect(
  CodexReplay.CodexAppServerReplayTranscript,
);

const report = () => {
  say(
    `PS011 tree=${LABEL} node=${process.version} arch=${process.arch} platform=${process.platform} load_ms=${LOAD_MS}`,
  );
  const started = new Map<number, Set<string>>();
  let staleStart: Mark | undefined;
  for (let index = 0; index < marks.length; index += 1) {
    const mark = marks[index]!;
    if (mark.phase === "start" && mark.method === "turn/started" && mark.turnId) {
      const set = started.get(mark.runtime) ?? new Set<string>();
      set.add(mark.turnId);
      started.set(mark.runtime, set);
    }
    if (mark.phase !== "start" || mark.method !== "thread/tokenUsage/updated") continue;
    const end = marks.find(
      (other, otherIndex) =>
        otherIndex > index &&
        other.phase === "end" &&
        other.runtime === mark.runtime &&
        other.method === mark.method &&
        other.turnId === mark.turnId,
    );
    const ownTurn = started.get(mark.runtime)?.has(mark.turnId ?? "") ?? false;
    if (!ownTurn && mark.runtime === 2) staleStart ??= mark;
    say(
      `PS011 usage-report runtime=${mark.runtime} turn=${mark.turnId} turn_started_in_this_runtime=${ownTurn ? "yes" : "no"} ` +
        `handler_tasks=${end ? end.tasks - mark.tasks : "?"} handler_dispatches=${end ? end.dispatches - mark.dispatches : "?"} ` +
        `handler_wall_ms=${end ? (end.ms - mark.ms).toFixed(2) : "?"}`,
    );
  }
  if (staleStart === undefined) {
    say("PS011 next-turn-start: no restored usage report seen in the second runtime");
    return;
  }
  const from = staleStart;
  const next = marks.find(
    (mark) =>
      mark.phase === "start" &&
      mark.runtime === 2 &&
      mark.method === "turn/started" &&
      mark.tasks >= from.tasks &&
      mark.ms >= from.ms,
  );
  if (next === undefined) {
    say("PS011 next-turn-start: second runtime's turn/started not seen");
    return;
  }
  say(
    `PS011 next-turn-start runtime=2 turn=${next.turnId} tasks_since_restored_report=${next.tasks - from.tasks} ` +
      `dispatches_since_restored_report=${next.dispatches - from.dispatches} wall_ms_since_restored_report=${(next.ms - from.ms).toFixed(2)}` +
      (LOAD_MS > 0 ? ` synthetic_busy_iterations=${ticks}` : ""),
  );
};

describe("ps011 runtime transcript", () => {
  it.effect("replays provider_thread_resume and reports the restored usage report's cost", () =>
    Effect.gen(function* () {
      const workspace = yield* checkpointWorkspace("provider_thread_resume");
      const raw = yield* readProviderReplayTranscript(
        new URL("./fixtures/provider_thread_resume/codex_transcript.ndjson", import.meta.url),
      );
      const transcript = yield* decodeCodexTranscript(
        materializeReplayTranscriptWorkspace(raw, workspace),
      );
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* Effect.acquireRelease(
        fs.makeTempDirectory({ prefix: "t3-ps011-transcript-" }),
        (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
      );
      const dbPath = path.join(tempDir, "state.sqlite");
      const driver = yield* CodexReplay.makeReplayDriver(transcript);
      const materialized = yield* materializeFixtureInput({
        scenario: "provider_thread_resume",
        fixtureInput: {
          steps: [
            { type: "message", text: PROVIDER_THREAD_RESUME_FIRST_PROMPT },
            { type: "message", text: PROVIDER_THREAD_RESUME_SECOND_PROMPT },
          ],
        },
        driver: ProviderDriverKind.make("codex"),
        modelSelection: CODEX_MODEL_SELECTION,
      });
      const splitIndex = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
      const phase1Steps = materialized.steps.slice(0, splitIndex + 1);
      const phase2Steps = materialized.steps.slice(splitIndex + 1);
      const commandsOf = (steps: typeof materialized.steps) =>
        steps.flatMap((step) => (step.type === "dispatch" ? [step.command] : []));
      const harness = {
        ...CodexOrchestratorReplayHarness,
        makeProviderAdapterRegistryLayer: () => probeLayer({ transcript, driver }),
      };
      const options = {
        databaseLayer: SqlitePersistence.layerFromPath(dbPath).pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      };
      yield* runOrchestratorV2ProviderReplayScenario(
        {
          name: "provider_thread_resume/codex:first-runtime",
          transcript,
          commands: commandsOf(phase1Steps),
          steps: phase1Steps,
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { cwd: workspace },
        },
        harness,
        options,
      );
      yield* runOrchestratorV2ProviderReplayScenario(
        {
          name: "provider_thread_resume/codex:second-runtime",
          transcript,
          commands: commandsOf(phase2Steps),
          steps: phase2Steps,
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { cwd: workspace },
        },
        harness,
        { ...options, recoverOnStartup: true },
      );
      ticking = false;
      report();
    }).pipe(
      Effect.ensuring(Effect.sync(() => (ticking = false))),
      Effect.provideService(Scheduler.Scheduler, countingScheduler),
      provideDeterministicTestRuntime,
      Effect.provide(
        Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      ),
    ),
  );
});
