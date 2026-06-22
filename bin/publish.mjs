#!/usr/bin/env node
// Pre-flight release script for resilient-ws.
//
// Runs a full publish checklist before handing off to `npm publish`:
//   - npm auth, git cleanliness, package.json sanity
//   - resilient-ws subtree is synced to the public mirror repo (the monorepo is
//     the single source of truth; the public repo is produced only by subtree push)
//   - registry lookup: is this version already published? is it newer than latest?
//   - typecheck / test / build, then verify the freshly built dist
//   - re-check the working tree to ensure the build left metadata/output clean
//   - inspect the actual tarball (npm pack) for missing or leaked files
//   - confirm, then publish (skipping lifecycle scripts since we just ran them)
//
// Usage:
//   node bin/publish.mjs [--dry-run] [--yes] [--sync] [--tag <tag>]
//                        [--otp <code>] [--allow-dirty] [--remote <name>]
//                        [--branch <name>] [--skip-subtree-check]
//
// Flags:
//   --dry-run            Run every check + `npm publish --dry-run`; never publishes.
//   --yes, -y            Skip the interactive confirmation prompt.
//   --sync               Push the resilient-ws subtree to the public mirror as
//                        part of the release (after all validation passes).
//                        Without it, an out-of-sync mirror blocks publishing.
//   --tag <tag>          dist-tag to publish under (default: latest). A non-latest
//                        tag relaxes the "must be newer than latest" ordering check.
//   --otp <code>         npm one-time password (2FA). Omit to let npm prompt.
//   --allow-dirty        Permit publishing with uncommitted changes in the package.
//   --remote <name>      Git remote of the public mirror repo (default: resilient-ws).
//   --branch <name>      Branch of the public mirror repo (default: main).
//   --skip-subtree-check Skip the subtree-sync verification (use with care).
//   --help, -h           Show this help.

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline/promises";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const PKG_DIR = resolve(SCRIPT_DIR, "..");

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

const COLOR = process.stdout.isTTY && !process.env.NO_COLOR;
const c = (code, s) => (COLOR ? `\x1b[${code}m${s}\x1b[0m` : s);
const bold = (s) => c("1", s);
const dim = (s) => c("2", s);
const green = (s) => c("32", s);
const red = (s) => c("31", s);
const yellow = (s) => c("33", s);
const cyan = (s) => c("36", s);

let stepNo = 0;
const warnings = [];

const step = (msg) => process.stdout.write(`${dim(`[${++stepNo}]`)} ${msg} `);
const ok = (detail = "") => console.log(green("✓") + (detail ? " " + dim(detail) : ""));
const warn = (msg) => {
  warnings.push(msg);
  console.log(yellow("! ") + yellow(msg));
};
const info = (msg) => console.log("    " + dim(msg));

class PublishError extends Error {}
const fail = (msg) => {
  throw new PublishError(msg);
};

// ---------------------------------------------------------------------------
// Command helpers
// ---------------------------------------------------------------------------

/** Run a command, inheriting stdio (for typecheck/test/build/publish). */
function run(cmd, args) {
  execFileSync(cmd, args, { cwd: PKG_DIR, stdio: "inherit" });
}

/** Run a command and capture stdout. Returns { ok, stdout, stderr, code }. */
function capture(cmd, args) {
  try {
    const stdout = execFileSync(cmd, args, {
      cwd: PKG_DIR,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return { ok: true, stdout, stderr: "", code: 0 };
  } catch (err) {
    return {
      ok: false,
      stdout: err.stdout?.toString() ?? "",
      stderr: err.stderr?.toString() ?? "",
      code: typeof err.status === "number" ? err.status : 1,
    };
  }
}

// ---------------------------------------------------------------------------
// Semver
// ---------------------------------------------------------------------------

function parseSemver(v) {
  const m = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/.exec(v);
  if (!m) return null;
  return {
    major: +m[1],
    minor: +m[2],
    patch: +m[3],
    pre: m[4] ? m[4].split(".") : [],
  };
}

/** Returns -1 | 0 | 1, or null if either version is unparseable. */
function cmpSemver(a, b) {
  const A = parseSemver(a);
  const B = parseSemver(b);
  if (!A || !B) return null;
  for (const k of ["major", "minor", "patch"]) {
    if (A[k] !== B[k]) return A[k] < B[k] ? -1 : 1;
  }
  if (A.pre.length === 0 && B.pre.length === 0) return 0;
  if (A.pre.length === 0) return 1; // release > prerelease
  if (B.pre.length === 0) return -1;
  const len = Math.max(A.pre.length, B.pre.length);
  for (let i = 0; i < len; i++) {
    const x = A.pre[i];
    const y = B.pre[i];
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    const xn = /^\d+$/.test(x);
    const yn = /^\d+$/.test(y);
    if (xn && yn) {
      if (+x !== +y) return +x < +y ? -1 : 1;
    } else if (xn) return -1;
    else if (yn) return 1;
    else if (x !== y) return x < y ? -1 : 1;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const opts = {
    dryRun: false,
    yes: false,
    tag: "latest",
    otp: null,
    allowDirty: false,
    sync: false,
    remote: "resilient-ws",
    branch: "main",
    skipSubtreeCheck: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    switch (a) {
      case "--dry-run":
        opts.dryRun = true;
        break;
      case "--yes":
      case "-y":
        opts.yes = true;
        break;
      case "--allow-dirty":
        opts.allowDirty = true;
        break;
      case "--sync":
        opts.sync = true;
        break;
      case "--skip-subtree-check":
        opts.skipSubtreeCheck = true;
        break;
      case "--remote":
        opts.remote = argv[++i];
        if (!opts.remote) fail("--remote requires a value");
        break;
      case "--branch":
        opts.branch = argv[++i];
        if (!opts.branch) fail("--branch requires a value");
        break;
      case "--tag":
        opts.tag = argv[++i];
        if (!opts.tag) fail("--tag requires a value");
        break;
      case "--otp":
        opts.otp = argv[++i];
        if (!opts.otp) fail("--otp requires a value");
        break;
      case "--help":
      case "-h":
        opts.help = true;
        break;
      default:
        fail(`unknown argument: ${a}`);
    }
  }
  return opts;
}

const HELP = `resilient-ws release script

Usage: node bin/publish.mjs [options]

Options:
  --dry-run       Run all checks and \`npm publish --dry-run\`; never publishes.
  --yes, -y       Skip the confirmation prompt.
  --sync          Push the resilient-ws subtree to the public mirror during release.
  --tag <tag>     dist-tag (default: latest). Non-latest relaxes ordering check.
  --otp <code>    npm 2FA one-time password.
  --allow-dirty   Allow uncommitted changes in the package directory.
  --remote <name> Public mirror git remote (default: resilient-ws).
  --branch <name> Public mirror branch (default: main).
  --skip-subtree-check  Skip the subtree-sync verification.
  --help, -h      Show this help.`;

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

function loadPackageJson() {
  const path = resolve(PKG_DIR, "package.json");
  if (!existsSync(path)) fail("package.json not found");
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    fail(`package.json is not valid JSON: ${err.message}`);
  }
}

function checkPackageJson(pkg) {
  step("Validating package.json fields");
  const problems = [];
  if (!pkg.name) problems.push("missing \"name\"");
  if (!pkg.version) problems.push("missing \"version\"");
  else if (!parseSemver(pkg.version)) problems.push(`invalid version "${pkg.version}"`);
  if (!pkg.license) problems.push("missing \"license\"");
  if (!pkg.description) problems.push("missing \"description\"");
  if (pkg.type !== "module") problems.push('"type" must be "module"');
  if (pkg.private) problems.push('"private": true blocks publishing');

  const files = Array.isArray(pkg.files) ? pkg.files : [];
  for (const required of ["dist", "DESIGN.md"]) {
    if (!files.includes(required)) problems.push(`"files" must include "${required}"`);
  }

  const dot = pkg.exports?.["."];
  if (!dot?.types || !dot?.default) {
    problems.push('exports["."] must define "types" and "default"');
  }

  if (problems.length) fail("package.json issues:\n      - " + problems.join("\n      - "));
  ok(`${pkg.name}@${pkg.version}`);
}

function checkRequiredFiles() {
  step("Checking required files exist");
  const missing = ["README.md", "LICENSE", "DESIGN.md"].filter(
    (f) => !existsSync(resolve(PKG_DIR, f)),
  );
  if (missing.length) fail(`missing files: ${missing.join(", ")}`);
  ok();
}

function checkNpmAuth() {
  step("Checking npm authentication");
  const who = capture("npm", ["whoami"]);
  if (!who.ok) {
    fail("not logged in to npm — run `npm login` first");
  }
  ok(`as ${who.stdout.trim()}`);
}

function checkGitClean(allowDirty) {
  step("Checking git working tree");
  const inRepo = capture("git", ["rev-parse", "--is-inside-work-tree"]);
  if (!inRepo.ok) {
    ok("not a git repo, skipped");
    return;
  }
  const status = capture("git", ["status", "--porcelain", "--", "."]);
  const dirty = status.stdout.trim();
  if (dirty) {
    if (allowDirty) {
      warn("uncommitted changes present (--allow-dirty)");
    } else {
      fail(
        "uncommitted changes in package directory — commit them or pass --allow-dirty\n" +
          dirty
            .split("\n")
            .map((l) => "      " + l)
            .join("\n"),
      );
    }
  } else {
    ok("clean");
  }
}

const indent = (s) =>
  s
    .split("\n")
    .map((l) => "      " + l)
    .join("\n");

/**
 * Resolve whether the public mirror repo is in sync with the resilient-ws
 * subtree.
 *
 * The monorepo is the single source of truth; the public repo is produced
 * solely by `git subtree push --prefix=<pkg> <remote> <branch>`. A subtree
 * split produces synthetic commits whose tree is exactly the prefix subtree,
 * so the public repo's HEAD tree must equal `HEAD:<prefix>` here. Comparing
 * those two tree object ids is exact and far cheaper than re-running split.
 *
 * Returns null when skipped, otherwise a state object the caller uses to
 * decide whether to block, or to push the subtree (with --sync).
 */
function resolveSubtreeState(opts) {
  step("Checking resilient-ws subtree sync state");
  if (opts.skipSubtreeCheck) {
    ok("skipped (--skip-subtree-check)");
    return null;
  }
  const inRepo = capture("git", ["rev-parse", "--is-inside-work-tree"]);
  if (!inRepo.ok) {
    fail("not inside a git repo — cannot verify subtree sync (use --skip-subtree-check to override)");
  }
  const topRes = capture("git", ["rev-parse", "--show-toplevel"]);
  if (!topRes.ok) fail("could not resolve git repo root");
  const top = topRes.stdout.trim();

  let prefix = PKG_DIR.startsWith(top) ? PKG_DIR.slice(top.length) : null;
  if (prefix === null) fail(`package dir ${PKG_DIR} is not inside repo root ${top}`);
  prefix = prefix.replace(/^[/\\]+/, "").split("\\").join("/");

  const remoteUrl = capture("git", ["-C", top, "remote", "get-url", opts.remote]);
  if (!remoteUrl.ok) {
    fail(
      `git remote "${opts.remote}" is not configured — the public mirror repo must exist first.\n` +
        `      Create github.com/cch137/resilient-ws, then add the remote:\n` +
        `        git remote add ${opts.remote} git@github.com:cch137/resilient-ws.git\n` +
        `      (override with --remote <name>, or bypass with --skip-subtree-check)`,
    );
  }

  const fetched = capture("git", ["-C", top, "fetch", opts.remote, opts.branch]);
  if (!fetched.ok) {
    fail(
      `could not fetch ${opts.remote}/${opts.branch}:\n` +
        indent((fetched.stderr || fetched.stdout).trim()) +
        `\n      Ensure the public repo exists and you have push/pull access.`,
    );
  }

  const localTreeRes = capture("git", ["-C", top, "rev-parse", `HEAD:${prefix}`]);
  if (!localTreeRes.ok) {
    fail(`could not read subtree at HEAD:${prefix} — commit your changes first`);
  }
  const remoteTreeRes = capture("git", ["-C", top, "rev-parse", "FETCH_HEAD^{tree}"]);
  if (!remoteTreeRes.ok) fail("could not read fetched public repo tree");

  const localTree = localTreeRes.stdout.trim();
  const remoteTree = remoteTreeRes.stdout.trim();
  const state = {
    top,
    prefix,
    remote: opts.remote,
    branch: opts.branch,
    localTree,
    remoteTree,
    inSync: localTree === remoteTree,
  };

  if (state.inSync) ok(`${opts.remote}/${opts.branch} matches the subtree at HEAD`);
  else console.log(yellow("out of sync"));
  return state;
}

/**
 * Push the resilient-ws subtree to the public mirror (only with --sync, and
 * only after all validation has passed), then verify the mirror now matches
 * HEAD.
 */
function syncSubtree(opts, state) {
  const { top, prefix, remote, branch, localTree } = state;
  if (opts.dryRun) {
    step("Subtree push (dry run)");
    ok(`would run: git subtree push --prefix=${prefix} ${remote} ${branch}`);
    return;
  }

  step("Pushing resilient-ws subtree to public repo");
  console.log();
  try {
    run("git", ["-C", top, "subtree", "push", `--prefix=${prefix}`, remote, branch]);
  } catch (err) {
    fail(`git subtree push failed: ${err?.message || err}`);
  }
  ok();

  step("Verifying mirror matches HEAD after push");
  const fetched = capture("git", ["-C", top, "fetch", remote, branch]);
  if (!fetched.ok) fail("could not re-fetch mirror after push");
  const remoteTreeRes = capture("git", ["-C", top, "rev-parse", "FETCH_HEAD^{tree}"]);
  if (!remoteTreeRes.ok) fail("could not read mirror tree after push");
  if (remoteTreeRes.stdout.trim() !== localTree) {
    fail("mirror still out of sync after subtree push — investigate manually");
  }
  ok(`${remote}/${branch} now matches HEAD:${prefix}`);
}

/** After build/typecheck, ensure nothing tracked got dirtied (metadata/output). */
function checkCleanAfterBuild(opts) {
  step("Re-checking working tree after build");
  const inRepo = capture("git", ["rev-parse", "--is-inside-work-tree"]);
  if (!inRepo.ok) {
    ok("not a git repo, skipped");
    return;
  }
  const status = capture("git", ["status", "--porcelain", "--", "."]);
  const dirty = status.stdout.trim();
  if (!dirty) {
    ok("clean");
    return;
  }
  if (opts.allowDirty) {
    warn("build modified tracked files (--allow-dirty)\n" + indent(dirty));
  } else {
    fail(
      "build/typecheck modified tracked files — package metadata or output must be committed:\n" +
        indent(dirty),
    );
  }
}

/** Returns { published: string[]|null }. null = never published (404). */
function fetchRegistryVersions(name) {
  step("Querying npm registry");
  const res = capture("npm", ["view", name, "versions", "--json"]);
  if (!res.ok) {
    const combined = res.stdout + res.stderr;
    if (/E404|404 Not Found|not found/i.test(combined)) {
      ok("not published yet — this will be the first release");
      return { published: null };
    }
    fail(`npm view failed:\n${combined.trim()}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(res.stdout);
  } catch {
    fail(`could not parse npm view output:\n${res.stdout}`);
  }
  const versions = Array.isArray(parsed) ? parsed : [parsed];
  ok(`${versions.length} published version(s)`);
  return { published: versions };
}

function checkVersion(pkg, published, tag) {
  step("Checking version against registry");
  if (published === null) {
    ok(`first publish of ${pkg.version}`);
    return;
  }
  if (published.includes(pkg.version)) {
    fail(
      `version ${pkg.version} is already published — bump the version in package.json`,
    );
  }
  const latest = published.reduce((max, v) => (cmpSemver(v, max) > 0 ? v : max), published[0]);
  const cmp = cmpSemver(pkg.version, latest);
  if (cmp === null) {
    warn(`could not compare ${pkg.version} against latest ${latest}`);
  } else if (cmp <= 0) {
    if (tag !== "latest") {
      warn(`version ${pkg.version} is not newer than latest ${latest} (publishing under --tag ${tag})`);
    } else {
      fail(
        `version ${pkg.version} is not newer than latest published ${latest} — ` +
          `bump it, or publish under a non-latest --tag`,
      );
    }
  } else {
    ok(`${pkg.version} > latest ${latest}`);
  }
  if (parseSemver(pkg.version).pre.length && tag === "latest") {
    warn(`prerelease ${pkg.version} would be tagged "latest" — consider --tag next`);
  }
}

function runBuildPipeline() {
  step("Running typecheck");
  console.log();
  run("npm", ["run", "--silent", "typecheck"]);
  ok();

  step("Running tests");
  console.log();
  run("npm", ["test", "--silent"]);
  ok();

  step("Building dist");
  console.log();
  run("npm", ["run", "--silent", "build"]);
  ok();

  step("Verifying build output");
  const missing = ["dist/index.js", "dist/index.d.ts"].filter(
    (f) => !existsSync(resolve(PKG_DIR, f)),
  );
  if (missing.length) fail(`build did not produce: ${missing.join(", ")}`);
  ok();
}

function inspectTarball() {
  step("Inspecting publish tarball");
  const res = capture("npm", ["pack", "--dry-run", "--json"]);
  if (!res.ok) fail(`npm pack failed:\n${(res.stdout + res.stderr).trim()}`);
  let parsed;
  try {
    parsed = JSON.parse(res.stdout);
  } catch {
    fail(`could not parse npm pack output:\n${res.stdout}`);
  }
  const entry = Array.isArray(parsed) ? parsed[0] : parsed;
  const paths = (entry?.files ?? []).map((f) => f.path);

  const required = [
    "package.json",
    "README.md",
    "LICENSE",
    "DESIGN.md",
    "dist/index.js",
    "dist/index.d.ts",
  ];
  const missing = required.filter((r) => !paths.includes(r));
  if (missing.length) fail(`tarball is missing: ${missing.join(", ")}`);

  // Guard against shipping source: only .d.ts TypeScript is allowed.
  const leaked = paths.filter(
    (p) =>
      p.startsWith("src/") ||
      p.startsWith("test/") ||
      (p.endsWith(".ts") && !p.endsWith(".d.ts")),
  );
  if (leaked.length) fail(`tarball leaks source files: ${leaked.join(", ")}`);

  const size = entry?.size ?? 0;
  const unpacked = entry?.unpackedSize ?? 0;
  ok(`${paths.length} files, ${(size / 1024).toFixed(1)} kB packed`);
  for (const p of paths) info(p);
  info(`unpacked: ${(unpacked / 1024).toFixed(1)} kB`);
}

async function confirm(pkg, opts) {
  const target = `${bold(pkg.name)}@${bold(pkg.version)} → tag ${bold(opts.tag)}`;
  if (opts.dryRun) {
    console.log("\n" + cyan(`Dry run: would publish ${target}`));
    return true;
  }
  console.log("\n" + cyan(`About to publish ${target} to the npm registry.`));
  if (opts.yes) return true;
  if (!process.stdin.isTTY) {
    fail("not a TTY and --yes not given; aborting for safety");
  }
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  const answer = (await rl.question("Proceed? (y/N) ")).trim().toLowerCase();
  rl.close();
  return answer === "y" || answer === "yes";
}

function publish(opts) {
  const args = ["publish", "--tag", opts.tag];
  if (opts.otp) args.push("--otp", opts.otp);
  if (opts.dryRun) {
    args.push("--dry-run");
  } else {
    // Checks above already ran typecheck/test/build on a fresh dist; skip the
    // prepublishOnly lifecycle so we don't run them a second time.
    args.push("--ignore-scripts");
  }
  step(opts.dryRun ? "npm publish --dry-run" : "Publishing");
  console.log();
  run("npm", args);
  ok();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    console.log(HELP);
    return;
  }

  console.log(bold(`\nresilient-ws release — ${PKG_DIR}\n`));

  const pkg = loadPackageJson();

  checkPackageJson(pkg);
  checkRequiredFiles();
  checkNpmAuth();
  checkGitClean(opts.allowDirty);
  const subtree = resolveSubtreeState(opts);
  if (subtree && !subtree.inSync && !opts.sync) {
    const msg =
      `public repo ${subtree.remote}/${subtree.branch} is out of sync with the resilient-ws subtree.\n` +
      `      The monorepo is the source of truth — push the latest subtree first:\n` +
      `        git subtree push --prefix=${subtree.prefix} ${subtree.remote} ${subtree.branch}\n` +
      `      or re-run with --sync to push it as part of this release.`;
    if (opts.dryRun) warn(msg);
    else fail(msg);
  }
  const { published } = fetchRegistryVersions(pkg.name);
  checkVersion(pkg, published, opts.tag);
  runBuildPipeline();
  checkCleanAfterBuild(opts);
  inspectTarball();

  // Push the validated source to the public mirror before publishing, so we
  // never mirror code that failed typecheck/test/build.
  if (opts.sync && subtree && !subtree.inSync) syncSubtree(opts, subtree);

  if (warnings.length) {
    console.log("\n" + yellow(`${warnings.length} warning(s):`));
    for (const w of warnings) console.log(yellow("  ! ") + w);
  }

  const proceed = await confirm(pkg, opts);
  if (!proceed) {
    console.log(dim("\nAborted; nothing published."));
    process.exitCode = 1;
    return;
  }

  publish(opts);
  console.log(
    green(
      opts.dryRun
        ? `\nDry run complete — no changes published.`
        : `\nPublished ${pkg.name}@${pkg.version} (tag ${opts.tag}).`,
    ),
  );
}

main().catch((err) => {
  if (err instanceof PublishError) {
    console.log(red("\n✗ " + err.message));
  } else {
    console.log(red("\n✗ unexpected error: " + (err?.stack || err?.message || err)));
  }
  process.exitCode = 1;
});
