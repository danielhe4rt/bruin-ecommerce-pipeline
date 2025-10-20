import { H3Event, getQuery, sendError } from 'h3'
import { promises as fs } from 'node:fs'
import path from 'node:path'

function extToLang(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase()
  switch (ext) {
    case '.sql': return 'sql'
    case '.yml':
    case '.yaml': return 'yaml'
    case '.json': return 'json'
    case '.md': return 'md'
    default: return ''
  }
}

function isSafeChild(baseDir: string, target: string) {
  const rel = path.relative(baseDir, target)
  return !!rel && !rel.startsWith('..') && !path.isAbsolute(rel)
}

async function readFileSafe(baseDir: string, relPath: string) {
  const abs = path.join(baseDir, relPath)
  if (!isSafeChild(baseDir, abs)) {
    throw Object.assign(new Error('Invalid path'), { statusCode: 400 })
  }
  const stat = await fs.stat(abs)
  if (!stat.isFile()) {
    throw Object.assign(new Error('Not a file'), { statusCode: 400 })
  }
  const code = await fs.readFile(abs, 'utf8')
  return { code, abs }
}

async function listFilesSafe(baseDir: string, relDir: string, pattern?: string, recursive = true) {
  const absDir = path.join(baseDir, relDir)
  if (!isSafeChild(baseDir, absDir)) {
    throw Object.assign(new Error('Invalid directory'), { statusCode: 400 })
  }
  const stat = await fs.stat(absDir)
  if (!stat.isDirectory()) {
    throw Object.assign(new Error('Not a directory'), { statusCode: 400 })
  }
  const entries = await fs.readdir(absDir, { withFileTypes: true })
  const files: string[] = []
  for (const e of entries) {
    if (e.isFile()) {
      files.push(path.join(relDir, e.name))
    } else if (recursive && e.isDirectory()) {
      const sub = await listFilesSafe(baseDir, path.join(relDir, e.name), pattern, recursive)
      files.push(...sub)
    }
  }
  // simple pattern support: *.ext only
  let filtered = files
  if (pattern && pattern.startsWith('*.')) {
    const ext = pattern.slice(1) // e.g. '.sql'
    filtered = files.filter(f => f.toLowerCase().endsWith(ext.toLowerCase()))
  }
  filtered.sort((a, b) => a.localeCompare(b))
  return filtered
}

export default defineEventHandler(async (event: H3Event) => {
  try {
    const q = getQuery(event)
    const raw = String(q.path || '')
    const glob = q.glob ? String(q.glob) : undefined
    const recursive = q.recursive ? q.recursive === '1' || q.recursive === 'true' : true
    if (!raw) {
      throw Object.assign(new Error('Missing path'), { statusCode: 400 })
    }
    const baseDir = path.join(process.cwd(), 'content', 'samples')

    // Determine if path is file or directory
    const abs = path.join(baseDir, raw)
    if (!isSafeChild(baseDir, abs)) {
      throw Object.assign(new Error('Invalid path'), { statusCode: 400 })
    }
    const st = await fs.stat(abs).catch(() => null)
    if (st && st.isDirectory()) {
      const files = await listFilesSafe(baseDir, raw, glob, recursive)
      const results = await Promise.all(files.map(async (rel) => {
        const { code } = await readFileSafe(baseDir, rel)
        return {
          path: rel.replace(/\\/g, '/'),
          label: rel.replace(/\\/g, '/'),
          language: extToLang(rel),
          code
        }
      }))
      return { files: results }
    } else {
      const { code } = await readFileSafe(baseDir, raw)
      return {
        files: [{
          path: raw.replace(/\\/g, '/'),
          label: (q.label ? String(q.label) : raw).replace(/\\/g, '/'),
          language: extToLang(raw),
          code
        }]
      }
    }
  } catch (err: any) {
    const status = err?.statusCode || 500
    return sendError(event, createError({ statusCode: status, statusMessage: err?.message || 'Server error' }))
  }
})
