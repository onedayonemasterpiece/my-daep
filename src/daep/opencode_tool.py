from __future__ import annotations


def tool_source() -> str:
    return r'''import { tool } from "@opencode-ai/plugin"

const schema = tool.schema

function env(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

async function api(path: string, init: RequestInit = {}) {
  const base = env("DAEP_URL").replace(/\/$/, "")
  const token = env("DAEP_CONTROL_TOKEN")
  const headers = new Headers(init.headers)
  headers.set("authorization", `Bearer ${token}`)
  headers.set("content-type", "application/json")
  const response = await fetch(base + path, { ...init, headers })
  const text = await response.text()
  if (!response.ok) throw new Error(`DAEP ${response.status}: ${text.slice(-2000)}`)
  return text ? JSON.parse(text) : {}
}

async function git(worktree: string, args: string[]) {
  const proc = Bun.spawn(["git", "-C", worktree, ...args], { stdout: "pipe", stderr: "pipe" })
  const stdout = await new Response(proc.stdout).text()
  const stderr = await new Response(proc.stderr).text()
  const code = await proc.exited
  if (code !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr.slice(-1200)}`)
  return stdout.trim()
}

function repositoryFromRemote(remote: string): string {
  const cleaned = remote.trim().replace(/\.git$/, "")
  const ssh = cleaned.match(/^git@github\.com:(.+\/.+)$/)
  if (ssh) return ssh[1]
  const https = cleaned.match(/^https:\/\/github\.com\/(.+\/.+)$/)
  if (https) return https[1]
  throw new Error(`origin is not a github.com repository: ${remote}`)
}

const taskSpec = schema.object({
  name: schema.string().min(1),
  instruction: schema.string().min(1),
  model: schema.string().min(1),
  depends_on: schema.array(schema.string()).default([]),
  fallback_models: schema.array(schema.string()).default([]),
  max_attempts: schema.number().int().min(1).max(8).default(3),
})

const checkSpec = schema.object({
  name: schema.string().min(1),
  command: schema.string().min(1),
  outcome: schema.string().min(1),
  detail: schema.string().optional(),
  metadata: schema.record(schema.string(), schema.any()).optional(),
})

export default tool({
  description: "Run and control explicit DAEP distributed coding jobs on Kaggle for the current GitHub worktree. Use submit only when the user explicitly requests distributed/DAEP execution; do not silently replace distributed execution with local coding.",
  args: {
    action: schema.enum(["submit", "status", "follow", "resume", "attach", "cancel", "export", "finalize"]),
    job_id: schema.string().optional(),
    max_workers: schema.number().int().min(1).max(4).optional(),
    idempotency_key: schema.string().optional(),
    tasks: schema.array(taskSpec).optional(),
    after: schema.number().int().min(0).optional(),
    result_sha: schema.string().optional(),
    checks: schema.array(checkSpec).optional(),
  },
  async execute(args, context) {
    if (args.action === "submit") {
      if (!args.tasks?.length) throw new Error("submit requires a non-empty structured tasks plan")
      const worktree = context.worktree || context.directory
      const remote = await git(worktree, ["config", "--get", "remote.origin.url"])
      const repository = repositoryFromRemote(remote)
      const base_sha = await git(worktree, ["rev-parse", "HEAD"])
      const base_branch = await git(worktree, ["branch", "--show-current"])
      if (!base_branch) throw new Error("DAEP v1 requires an explicit checked-out Git branch/checkpoint")
      const dirty = await git(worktree, ["status", "--porcelain"])
      if (dirty) throw new Error("worktree has uncommitted changes; create an explicit Git checkpoint before distributed execution")
      const key = args.idempotency_key || `${context.sessionID}:${base_sha}:${JSON.stringify(args.tasks)}`
      return JSON.stringify(await api("/v1/jobs", {
        method: "POST",
        body: JSON.stringify({
          repository,
          base_sha,
          base_branch,
          idempotency_key: key,
          max_workers: args.max_workers || 2,
          coordinator_session_id: context.sessionID,
          coordinator_server_url: process.env.DAEP_OPENCODE_SERVER_URL || undefined,
          tasks: args.tasks,
        }),
      }), null, 2)
    }

    if (!args.job_id) throw new Error(`${args.action} requires job_id`)
    if (args.action === "status") return JSON.stringify(await api(`/v1/jobs/${args.job_id}`), null, 2)
    if (args.action === "follow") return JSON.stringify(await api(`/v1/jobs/${args.job_id}/events?after=${args.after || 0}`), null, 2)
    if (args.action === "resume" || args.action === "attach") {
      return JSON.stringify(await api(`/v1/jobs/${args.job_id}/attach`, {
        method: "POST",
        body: JSON.stringify({ session_id: context.sessionID, server_url: process.env.DAEP_OPENCODE_SERVER_URL || null }),
      }), null, 2)
    }
    if (args.action === "cancel") return JSON.stringify(await api(`/v1/jobs/${args.job_id}/cancel`, { method: "POST" }), null, 2)
    if (args.action === "export") return JSON.stringify(await api(`/v1/jobs/${args.job_id}/export`), null, 2)
    if (args.action === "finalize") {
      if (!args.result_sha || !args.checks?.length) throw new Error("finalize requires result_sha and at least one actual check result")
      return JSON.stringify(await api(`/v1/jobs/${args.job_id}/finalize`, {
        method: "POST",
        body: JSON.stringify({ result_sha: args.result_sha, checks: args.checks }),
      }), null, 2)
    }
    throw new Error(`unsupported action: ${args.action}`)
  },
})
'''
