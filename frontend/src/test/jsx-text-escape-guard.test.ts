import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative, sep } from 'node:path'

/**
 * jsx-text-escape-guard — JSX text does not process escapes, so `·` between
 * tags renders as those six characters. ActivityPanel showed one in every row.
 * Inside a string, a template or a comment an escape is fine, so those are
 * removed before looking.
 */
const ROOT = join(process.cwd(), 'src')

function tsxFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    if (statSync(path).isDirectory()) return tsxFiles(path)
    return name.endsWith('.tsx') ? [path] : []
  })
}

/** One line with its comments and quoted text gone. */
function withoutStrings(line: string): string {
  return line
    .replace(/\{\/\*.*?\*\/\}/g, '')
    .replace(/\/\*.*?\*\//g, '')
    .replace(/^\s*(\/\/|\*).*$/, '')
    .replace(/'(?:[^'\\\n]|\\.)*'/g, '')
    .replace(/"(?:[^"\\\n]|\\.)*"/g, '')
    .replace(/`(?:[^`\\\n]|\\.)*`/g, '')
}

describe('jsx-text-escape-guard', () => {
  it('no escape sequence sits in JSX text', () => {
    const offenders = tsxFiles(ROOT).flatMap((path) => {
      const rel = relative(ROOT, path).split(sep).join('/')
      return readFileSync(path, 'utf8')
        .split('\n')
        .flatMap((line, index) =>
          /\\u[0-9a-fA-F]{4}/.test(withoutStrings(line)) ? [`${rel}:${index + 1}`] : [],
        )
    })
    expect(offenders).toEqual([])
  })
})
