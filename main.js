'use strict'

const mineflayer = require('mineflayer')
const readline = require('readline')
const fs = require('fs')
const path = require('path')
const net = require('net')
const http = require('http')
const os = require('os')
const EventEmitter = require('events')
const config = require('./config.json')
const chalk = require('chalk')
const Table = require('cli-table3')

const AUTO_EXE = config.autoExe === true

let expressApp, expressServer, io
try {
  const express = require('express')
  const socketIo = require('socket.io')
  expressApp = express()
  expressServer = http.createServer(expressApp)
  io = new socketIo.Server(expressServer, { cors: { origin: '*' } })
} catch (e) {
  if (!AUTO_EXE) console.warn('[Dashboard] express/socket.io không khả dụng:', e.message)
}

const rand = (a, b) => Math.floor(Math.random() * (b - a + 1)) + a
const jit = (base, s) => Math.max(0, base + Math.round((Math.random() * 2 - 1) * s))
const sleep = ms => new Promise(r => setTimeout(r, ms))
const clamp = (v, a, b) => Math.min(b, Math.max(a, v))
const nowMs = () => Date.now()

let _saveConfigTimer = null
function saveConfigDebounced() {
  if (_saveConfigTimer) clearTimeout(_saveConfigTimer)
  _saveConfigTimer = setTimeout(() => {
    _saveConfigTimer = null
    try {
      fs.writeFileSync(path.join(process.cwd(), 'config.json'), JSON.stringify(config, null, 2), 'utf8')
    } catch (e) {
      if (!AUTO_EXE) console.error('[Config] Lỗi ghi config.json:', e.message)
    }
  }, 1000)
}

function stripMc(s) {
  return String(s)
    .replace(/§[0-9a-fk-or]/gi, '')
    .replace(/\u00a7[0-9a-fk-or]/gi, '')
    .replace(/\\u[0-9a-fA-F]{4}/g, '')
}

function stripAnsi(str) {
  return String(str).replace(/\x1b\[[0-9;]*[A-Za-z]/g, '')
}

function resolveText(raw) {
  if (raw === null || raw === undefined) return ''
  if (typeof raw === 'string') return stripMc(raw)
  try {
    const o = typeof raw === 'object' ? raw : JSON.parse(raw)
    let out = o.text || ''
    if (Array.isArray(o.extra)) out += o.extra.map(resolveText).join('')
    if (Array.isArray(o.with)) out += o.with.map(resolveText).join('')
    return stripMc(out)
  } catch { return stripMc(String(raw)) }
}

function parseShardNum(text) {
  if (!text) return null
  const up = text.toUpperCase()
  if (!up.includes('SHARD') && !up.includes('MẢNH')) return null
  for (const m of text.matchAll(/(\d[\d,.]*)\s*(k|K|M)?/g)) {
    let n = parseFloat(m[1].replace(/[,.]/g, ''))
    if (isNaN(n)) continue
    if (m[2] === 'k' || m[2] === 'K') n *= 1e3
    if (m[2] === 'M') n *= 1e6
    if (n >= 1) return Math.round(n)
  }
  return null
}

function safeJsonStringify(obj, fallback = '{}') {
  try { return JSON.stringify(obj) } catch { return fallback }
}

const MAX_LOG_PER_BOT = 800

class RingBuffer {
  constructor(capacity = MAX_LOG_PER_BOT) {
    this._cap = capacity
    this._buf = new Array(capacity)
    this._head = 0
    this._size = 0
  }
  push(item) {
    this._buf[this._head % this._cap] = item
    this._head++
    if (this._size < this._cap) this._size++
  }
  toArray() {
    if (this._size === 0) return []
    const start = this._head - this._size
    const out = []
    for (let i = 0; i < this._size; i++) out.push(this._buf[(start + i) % this._cap])
    return out
  }
  clear() { this._head = 0; this._size = 0 }
}

const botLogs = new Map()
function getBotLog(id) {
  if (!botLogs.has(id)) botLogs.set(id, new RingBuffer())
  return botLogs.get(id)
}

const THEMES = {
  teal:   { border: [0, 200, 180],   accent: [0, 225, 150],   gA: [0, 220, 180],   gB: [80, 160, 255]  },
  blue:   { border: [70, 150, 255],  accent: [120, 185, 255], gA: [60, 140, 255],  gB: [120, 225, 255] },
  purple: { border: [150, 100, 230], accent: [195, 145, 255], gA: [150, 100, 230], gB: [255, 120, 210] },
  pink:   { border: [230, 110, 180], accent: [255, 150, 205], gA: [255, 120, 190], gB: [255, 190, 120] },
  green:  { border: [60, 200, 110],  accent: [105, 238, 155], gA: [60, 220, 140],  gB: [175, 255, 80]  },
  gold:   { border: [230, 190, 60],  accent: [255, 218, 95],  gA: [255, 205, 60],  gB: [255, 150, 60]  },
}
const TNAMES = Object.keys(THEMES)
const pickTheme = (name, i) => {
  const n = (name && THEMES[name]) ? name : TNAMES[((i % TNAMES.length) + TNAMES.length) % TNAMES.length]
  return { name: n, ...THEMES[n] }
}

function gradient(text, [r1, g1, b1], [r2, g2, b2]) {
  const chars = [...String(text)]
  const len = chars.length
  return chars.map((ch, i) => {
    const t = len < 2 ? 0 : i / (len - 1)
    return chalk.rgb(
      Math.round(r1 + (r2 - r1) * t),
      Math.round(g1 + (g2 - g1) * t),
      Math.round(b1 + (b2 - b1) * t)
    )(ch)
  }).join('')
}

function badge(level) {
  const map = {
    ok: chalk.bgRgb(0, 185, 110).rgb(0, 0, 0).bold(' ✓ '),
    warn: chalk.bgRgb(215, 130, 0).rgb(0, 0, 0).bold(' ▲ '),
    err: chalk.bgRgb(210, 50, 50).rgb(255, 255, 255).bold(' ✕ '),
    shard: chalk.bgRgb(0, 175, 220).rgb(0, 0, 0).bold(' ◈ '),
    chat: chalk.bgRgb(120, 70, 210).rgb(255, 255, 255).bold(' ✉ '),
    sys: chalk.bgRgb(30, 30, 55).rgb(130, 130, 195).bold(' · '),
    afk: chalk.bgRgb(200, 130, 0).rgb(0, 0, 0).bold(' ⌚ '),
    pkt: chalk.bgRgb(0, 130, 170).rgb(255, 255, 255).bold(' ⇄ '),
    proxy: chalk.bgRgb(80, 40, 160).rgb(255, 255, 255).bold(' ⬡ '),
    health: chalk.bgRgb(200, 60, 60).rgb(255, 255, 255).bold(' ♥ '),
  }
  return map[level] || map.sys
}

function bcol(level) {
  const map = {
    ok: chalk.rgb(50, 225, 140),
    warn: chalk.rgb(225, 170, 10),
    err: chalk.rgb(225, 80, 80),
    shard: chalk.rgb(30, 205, 230),
    chat: chalk.rgb(180, 125, 255),
    sys: chalk.rgb(110, 110, 170),
    afk: chalk.rgb(225, 170, 10),
    pkt: chalk.rgb(0, 195, 225),
    proxy: chalk.rgb(160, 110, 255),
    health: chalk.rgb(225, 90, 90),
  }
  return map[level] || map.sys
}

const ICONS = {
  online: chalk.rgb(0, 225, 95).bold('● ONLINE'),
  offline: chalk.rgb(210, 70, 70).bold('○ OFFLINE'),
  reconn: chalk.rgb(255, 180, 0).bold('↻ RECONNECTING'),
  spawn: chalk.rgb(0, 200, 255).bold('↑ SPAWNING'),
  afkJ: chalk.rgb(255, 200, 0).bold('JUMP'),
  afkW: chalk.rgb(255, 160, 0).bold('WALK'),
  none: chalk.dim.rgb(110, 110, 145)('—'),
}

const ts = () => {
  const d = new Date(), p = n => String(n).padStart(2, '0')
  return chalk.dim.rgb(80, 80, 120)(`${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`)
}

function createRoundBox(content, borderColorFn, title) {
  const lines = content.split('\n')
  const maxLen = Math.max(...lines.map(l => stripAnsi(l).length))
  const color = borderColorFn || chalk.cyan
  const titleStr = title ? ' ' + title + ' ' : ''
  const topBar = title
    ? color('╭─' + titleStr + '─'.repeat(Math.max(0, maxLen - stripAnsi(titleStr).length)) + '╮')
    : color('╭' + '─'.repeat(maxLen + 2) + '╮')
  const padded = lines.map(l => {
    const len = stripAnsi(l).length
    return color('│') + ' ' + l + ' '.repeat(maxLen - len) + ' ' + color('│')
  })
  return [topBar, ...padded, color('╰' + '─'.repeat(maxLen + 2) + '╯')].join('\n')
}

class UIManager {
  constructor() {
    this.rl = null
    this._rawWrite = process.stdout.write.bind(process.stdout)
    this.silent = AUTO_EXE
  }
  setReadline(rl) { this.rl = rl }
  printLine(text) {
    if (this.silent) return
    if (this.rl && !this.rl.closed) {
      this._rawWrite('\r\x1b[K')
      this._rawWrite(text + '\n')
      this.rl.prompt(true)
    } else {
      this._rawWrite(text + '\n')
    }
  }
  log(level, id, msg, theme) {
    if (this.silent) return
    const label = id ? chalk.rgb(...(theme ? theme.accent : [160, 160, 160])).bold(`${id} `) : ''
    this.printLine(`${ts()} ${badge(level)} ${label}${bcol(level)(msg)}`)
  }
  printBanner(insts) {
    if (this.silent) return
    const LOGO = [
      ' ███╗   ███╗██╗███╗   ██╗███████╗',
      ' ████╗ ████║██║████╗  ██║██╔════╝',
      ' ██╔████╔██║██║██╔██╗ ██║█████╗  ',
      ' ██║╚██╔╝██║██║██║╚██╗██║██╔══╝  ',
      ' ██║ ╚═╝ ██║██║██║ ╚████║███████╗',
      ' ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚══════╝',
    ]
    let logoStr = '\n'
    LOGO.forEach((line, i) => {
      const t = i / (LOGO.length - 1)
      const c2 = [Math.round(100 * t), Math.round(220 - 140 * t), Math.round(180 + 75 * t)]
      logoStr += chalk.bold(gradient(line, [0, 220, 180], c2)) + '\n'
    })
    this.printLine(logoStr)
    const table = new Table({
      head: [
        chalk.rgb(0, 220, 180).bold('◉'),
        chalk.rgb(240, 240, 255).bold('ID'),
        chalk.rgb(0, 215, 145).bold('SERVER'),
        chalk.rgb(110, 170, 255).bold('USER'),
      ],
      colWidths: [3, 14, 30, 22],
      style: { head: [], border: [], 'padding-left': 1, 'padding-right': 1 },
      chars: TABLE_CHARS,
    })
    insts.forEach(inst => {
      table.push([
        chalk.rgb(...inst.theme.accent).bold('◉'),
        chalk.rgb(240, 240, 255)(inst.cfg.id),
        chalk.rgb(0, 215, 145)(`${inst.cfg.host}:${inst.cfg.port}`),
        chalk.rgb(110, 170, 255)(inst.cfg.username),
      ])
    })
    this.printLine(createRoundBox(table.toString(), chalk.cyan, 'Antares Generator'))
  }
  printList(insts) {
    if (this.silent) return
    const table = new Table({
      head: ['', 'ID', 'STATE', 'AFK', 'PING', 'PROXY', 'SHARD', 'RECONN'].map(s =>
        chalk.rgb(240, 240, 255).bold(s)
      ),
      colWidths: [3, 14, 16, 8, 8, 24, 12, 8],
      style: { head: [], border: [], 'padding-left': 1, 'padding-right': 1 },
      chars: TABLE_CHARS,
    })
    insts.forEach(inst => {
      const s = inst.state
      const px = inst.proxy ? chalk.rgb(160, 110, 255)(`${inst.proxy.host}:${inst.proxy.port}`) : ICONS.none
      const sh = s.shard > 0 ? gradient(s.shard.toLocaleString(), inst.theme.gA, inst.theme.gB) : ICONS.none
      const stateIcon = { ONLINE: ICONS.online, RECONNECTING: ICONS.reconn, SPAWNING: ICONS.spawn }[s.connState] || ICONS.offline
      const pingStr = s.ping >= 0 ? chalk.rgb(0, 200, 130)(`${s.ping}ms`) : ICONS.none
      table.push([
        chalk.rgb(...inst.theme.accent).bold('◉'),
        chalk.rgb(240, 240, 255)(inst.cfg.id),
        stateIcon,
        s.afk === 'jump' ? ICONS.afkJ : s.afk === 'walk' ? ICONS.afkW : ICONS.none,
        pingStr, px, sh, chalk.dim(`${s.reconnects}`),
      ])
    })
    this.printLine(createRoundBox(table.toString(), chalk.cyan, 'DANH SÁCH BOT'))
  }
  printStatus(inst) {
    if (this.silent) return
    const s = inst.state
    const pm = inst.packetMgr
    const stateStr = {
      DISCONNECTED: chalk.rgb(210, 70, 70)('DISCONNECTED'),
      CONNECTING: chalk.rgb(255, 180, 0)('CONNECTING'),
      AUTHENTICATING: chalk.rgb(255, 200, 0)('AUTHENTICATING'),
      SPAWNING: chalk.rgb(0, 200, 255)('SPAWNING'),
      ONLINE: chalk.rgb(0, 225, 95)('ONLINE'),
      RECONNECTING: chalk.rgb(255, 180, 0)('RECONNECTING'),
      STOPPING: chalk.rgb(200, 60, 60)('STOPPING'),
    }[s.connState] || chalk.dim('UNKNOWN')
    const lines = [
      `${chalk.dim.rgb(130, 130, 175)('Server   ')}  ${chalk.rgb(0, 215, 145).bold(`${inst.cfg.host}:${inst.cfg.port}`)}`,
      `${chalk.dim.rgb(130, 130, 175)('Username ')}  ${chalk.rgb(110, 170, 255).bold(inst.cfg.username)}`,
      `${chalk.dim.rgb(130, 130, 175)('State    ')}  ${stateStr}`,
      `${chalk.dim.rgb(130, 130, 175)('AFK      ')}  ${s.afk === 'jump' ? ICONS.afkJ : s.afk === 'walk' ? ICONS.afkW : ICONS.none}`,
      `${chalk.dim.rgb(130, 130, 175)('Shard    ')}  ${s.shard > 0 ? gradient(s.shard.toLocaleString(), inst.theme.gA, inst.theme.gB) : ICONS.none}`,
      `${chalk.dim.rgb(130, 130, 175)('Ping     ')}  ${s.ping >= 0 ? chalk.rgb(0, 200, 130)(`${s.ping}ms`) : ICONS.none}`,
      `${chalk.dim.rgb(130, 130, 175)('Packets/s')}  ${pm ? chalk.rgb(0, 195, 225)(`${pm.ppsIn}↓ ${pm.ppsOut}↑`) : ICONS.none}`,
    ]
    this.printLine(createRoundBox(lines.join('\n'), chalk.rgb(...inst.theme.border), `STATUS  ${inst.cfg.id}`))
  }
  printShardDiff(inst, now, prev) {
    if (this.silent) return
    let line
    if (prev === 0) {
      line = `${chalk.rgb(120, 120, 160)('Lần đầu ghi nhận')}\n  ${gradient(now.toLocaleString(), inst.theme.gA, inst.theme.gB)}`
    } else if (now > prev) {
      line = `${chalk.rgb(0, 225, 100)(`▲  +${(now - prev).toLocaleString()} shard`)}\n${chalk.rgb(120, 120, 160)('Tổng  ')}${chalk.rgb(...inst.theme.accent).bold(now.toLocaleString())}`
    } else if (now < prev) {
      line = `${chalk.rgb(215, 65, 65)(`▼  −${(prev - now).toLocaleString()} shard`)}\n${chalk.rgb(120, 120, 160)('Tổng  ')}${chalk.rgb(...inst.theme.accent).bold(now.toLocaleString())}`
    } else {
      line = `${chalk.rgb(120, 120, 160)('Không thay đổi  ')}${chalk.rgb(...inst.theme.accent).bold(now.toLocaleString())}`
    }
    this.printLine(createRoundBox(line, chalk.rgb(...inst.theme.border), `◈ SHARD  ${inst.cfg.id}`))
  }
  printProxyList(proxyManager) {
    if (this.silent) return
    const list = proxyManager.list
    if (!list.length) {
      this.printLine(`${ts()} ${badge('proxy')} ${chalk.rgb(160, 110, 255)('Chưa có proxy nào')}`)
      return
    }
    const table = new Table({
      head: ['#', 'TYPE', 'HOST', 'PORT', 'AUTH'].map(s => chalk.rgb(255, 200, 0).bold(s)),
      colWidths: [4, 8, 30, 8, 6],
      style: { head: [], border: [], 'padding-left': 1, 'padding-right': 1 },
      chars: TABLE_CHARS,
    })
    list.forEach((p, i) => {
      table.push([
        chalk.rgb(255, 200, 0)(i),
        chalk.rgb(160, 110, 255)(p.type),
        chalk.rgb(0, 215, 145)(p.host),
        chalk.rgb(110, 170, 255)(p.port),
        p.user ? chalk.rgb(0, 200, 120)('✓') : ICONS.none,
      ])
    })
    this.printLine(createRoundBox(table.toString(), chalk.magenta, `DANH SÁCH PROXY  (${list.length})`))
  }
}

const TABLE_CHARS = {
  'top': '─', 'top-mid': '┬', 'top-left': '╭', 'top-right': '╮',
  'bottom': '─', 'bottom-mid': '┴', 'bottom-left': '╰', 'bottom-right': '╯',
  'left': '│', 'left-mid': '├', 'mid': '─', 'mid-mid': '┼',
  'right': '│', 'right-mid': '┤', 'middle': '│',
}

class ProxyManager {
  constructor() { this.list = []; this.index = 0 }
  parse(raw) {
    raw = String(raw).trim()
    if (!raw || raw.startsWith('#')) return null
    try {
      let str = raw
      if (!str.includes('://')) {
        const parts = str.split(':')
        if (parts.length === 4) {
          str = `socks5://${encodeURIComponent(parts[2])}:${encodeURIComponent(parts[3])}@${parts[0]}:${parts[1]}`
        } else { str = 'socks5://' + str }
      } else if (!/^https?:\/\/|^socks[45]:\/\//i.test(str)) {
        str = 'socks5://' + str
      }
      const u = new URL(str)
      const type = u.protocol.replace(':', '').toLowerCase()
      if (!['http', 'https', 'socks4', 'socks5'].includes(type)) return null
      return {
        type, host: u.hostname,
        port: parseInt(u.port, 10) || (type === 'https' ? 443 : 1080),
        user: u.username ? decodeURIComponent(u.username) : null,
        pass: u.password ? decodeURIComponent(u.password) : null,
        raw,
      }
    } catch { return null }
  }
  add(raw) {
    const p = this.parse(raw)
    if (!p) return { ok: false, msg: 'Proxy không hợp lệ: ' + raw }
    if (this.list.find(x => x.host === p.host && x.port === p.port))
      return { ok: false, msg: 'Proxy đã tồn tại: ' + p.host + ':' + p.port }
    this.list.push(p)
    return { ok: true, msg: `${p.type}://${p.host}:${p.port}` }
  }
  loadFile(filePath) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').split('\n')
      let added = 0, skip = 0
      for (const l of lines) {
        if (!l.trim()) continue
        const r = this.add(l)
        if (r.ok) added++
        else if (!l.trim().startsWith('#')) skip++
      }
      return { ok: true, added, skip }
    } catch (e) { return { ok: false, msg: e.message } }
  }
  next() {
    if (!this.list.length) return null
    const p = this.list[this.index % this.list.length]
    this.index++
    return p
  }
  remove(index) {
    if (index < 0 || index >= this.list.length) return null
    return this.list.splice(index, 1)[0]
  }
  async connect(proxy, targetHost, targetPort) {
    switch (proxy.type) {
      case 'http': case 'https': return this._connectHttp(proxy, targetHost, targetPort)
      case 'socks4': return this._connectSocks4(proxy, targetHost, targetPort)
      case 'socks5': return this._connectSocks5(proxy, targetHost, targetPort)
      default: throw new Error('Loại proxy không hỗ trợ: ' + proxy.type)
    }
  }
  _connectHttp(proxy, targetHost, targetPort) {
    return new Promise((resolve, reject) => {
      let settled = false
      const sock = net.connect(proxy.port, proxy.host)
      const timer = setTimeout(() => { try { sock.destroy() } catch { }; settle(new Error('HTTP proxy timeout')) }, TIMING.PROXY_TIMEOUT)
      const settle = (err, res) => { if (settled) return; settled = true; clearTimeout(timer); err ? reject(err) : resolve(res) }
      sock.once('connect', () => {
        try {
          let req = `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\nHost: ${targetHost}:${targetPort}\r\n`
          if (proxy.user) {
            const cred = Buffer.from(`${proxy.user}:${proxy.pass || ''}`).toString('base64')
            req += `Proxy-Authorization: Basic ${cred}\r\n`
          }
          req += '\r\n'
          sock.write(req)
        } catch (e) { settle(e) }
      })
      let buf = ''
      sock.on('data', d => {
        buf += d.toString('latin1')
        if (!buf.includes('\r\n\r\n')) return
        if (/^HTTP\/1\.[01] 200/i.test(buf)) settle(null, sock)
        else { sock.destroy(); settle(new Error('HTTP proxy: ' + buf.split('\r\n')[0])) }
      })
      sock.once('error', settle)
      sock.once('close', () => settle(new Error('HTTP proxy đóng sớm')))
    })
  }
  _connectSocks4(proxy, targetHost, targetPort) {
    return new Promise((resolve, reject) => {
      let settled = false
      const sock = net.connect(proxy.port, proxy.host)
      const timer = setTimeout(() => { try { sock.destroy() } catch { }; settle(new Error('SOCKS4 timeout')) }, TIMING.PROXY_TIMEOUT)
      const settle = (err, res) => { if (settled) return; settled = true; clearTimeout(timer); err ? reject(err) : resolve(res) }
      sock.once('connect', () => {
        try {
          const user = Buffer.from(proxy.user || '')
          const domain = Buffer.from(targetHost + '\0')
          const portBuf = Buffer.alloc(2)
          portBuf.writeUInt16BE(targetPort, 0)
          sock.write(Buffer.concat([
            Buffer.from([0x04, 0x01]), portBuf, Buffer.from([0, 0, 0, 1]),
            user, Buffer.from([0x00]), domain,
          ]))
        } catch (e) { settle(e) }
      })
      sock.once('data', d => {
        if (d.length >= 2 && d[1] === 0x5a) settle(null, sock)
        else { sock.destroy(); settle(new Error('SOCKS4 từ chối: ' + (d.length > 1 ? d[1] : '?'))) }
      })
      sock.once('error', settle)
      sock.once('close', () => settle(new Error('SOCKS4 đóng sớm')))
    })
  }
  _connectSocks5(proxy, targetHost, targetPort) {
    return new Promise((resolve, reject) => {
      let settled = false, step = 0
      const sock = net.connect(proxy.port, proxy.host)
      const timer = setTimeout(() => { try { sock.destroy() } catch { }; settle(new Error('SOCKS5 timeout')) }, TIMING.PROXY_TIMEOUT)
      const settle = (err, res) => { if (settled) return; settled = true; clearTimeout(timer); err ? reject(err) : resolve(res) }
      const sendConnect = () => {
        const host = Buffer.from(targetHost)
        const portBuf = Buffer.alloc(2)
        portBuf.writeUInt16BE(targetPort, 0)
        sock.write(Buffer.concat([Buffer.from([0x05, 0x01, 0x00, 0x03, host.length]), host, portBuf]))
      }
      const onData = d => {
        try {
          if (step === 0) {
            if (d[1] === 0xff) { sock.destroy(); settle(new Error('SOCKS5: không có auth phù hợp')); return }
            if (d[1] === 0x02 && proxy.user) {
              const u = Buffer.from(proxy.user || ''); const p = Buffer.from(proxy.pass || '')
              sock.write(Buffer.concat([Buffer.from([0x01, u.length]), u, Buffer.from([p.length]), p]))
              step = 1
            } else { sendConnect(); step = 2 }
          } else if (step === 1) {
            if (d[1] !== 0x00) { sock.destroy(); settle(new Error('SOCKS5 auth thất bại')); return }
            sendConnect(); step = 2
          } else if (step === 2) {
            if (d[1] !== 0x00) { sock.destroy(); settle(new Error('SOCKS5 từ chối: ' + d[1])); return }
            sock.removeListener('data', onData); settle(null, sock)
          }
        } catch (e) { settle(e) }
      }
      sock.once('connect', () => {
        try {
          const method = proxy.user ? Buffer.from([0x05, 0x02, 0x00, 0x02]) : Buffer.from([0x05, 0x01, 0x00])
          sock.write(method)
        } catch (e) { settle(e) }
      })
      sock.on('data', onData)
      sock.once('error', settle)
      sock.once('close', () => settle(new Error('SOCKS5 đóng sớm')))
    })
  }
}

class PacketManager {
  constructor(botId) {
    this.botId = botId; this.ppsIn = 0; this.ppsOut = 0
    this.lastPacketAt = null; this._cntIn = 0; this._cntOut = 0
    this._interval = null; this._mc = null; this._boundIn = null
  }
  attach(mc) {
    this.detach(); this._mc = mc
    this._cntIn = 0; this._cntOut = 0; this.ppsIn = 0; this.ppsOut = 0
    this.lastPacketAt = nowMs()
    this._boundIn = () => { this._cntIn++; this.lastPacketAt = nowMs() }
    const client = mc._client
    if (!client) return
    client.on('packet', this._boundIn)
    const origWrite = client.write.bind(client)
    client.write = (...args) => { this._cntOut++; return origWrite(...args) }
    this._interval = setInterval(() => {
      this.ppsIn = this._cntIn; this.ppsOut = this._cntOut; this._cntIn = this._cntOut = 0
    }, 1000)
  }
  detach() {
    if (this._interval) { clearInterval(this._interval); this._interval = null }
    if (this._mc?._client && this._boundIn) {
      try { this._mc._client.removeListener('packet', this._boundIn) } catch { }
    }
    this._mc = null; this._boundIn = null
    this.ppsIn = 0; this.ppsOut = 0; this.lastPacketAt = null
  }
  isStale(thresholdMs = 30000) {
    if (!this.lastPacketAt) return true
    return (nowMs() - this.lastPacketAt) > thresholdMs
  }
}

const CS = Object.freeze({
  DISCONNECTED: 'DISCONNECTED', CONNECTING: 'CONNECTING',
  AUTHENTICATING: 'AUTHENTICATING', SPAWNING: 'SPAWNING',
  ONLINE: 'ONLINE', RECONNECTING: 'RECONNECTING', STOPPING: 'STOPPING',
})

const TIMING = {
  RECONNECT_DELAYS: [1000, 2000, 5000, 10000, 20000, 30000, 60000],
  MAX_RECONNECT: 10, STABLE_TIME: 30000,
  HEALTH_INTERVAL: 15000, HEALTH_JITTER: 3000,
  PROXY_TIMEOUT: 8000, PACKET_TIMEOUT: 30000, ENTITY_TIMEOUT: 20000,
}

const IGNORED_ERRORS = [
  'PartialReadError', 'packet_world_particles', 'ECONNRESET', 'EPIPE',
  'Cannot read properties of null', 'read ECONNRESET', 'socket hang up',
  'write after end', 'This socket has been ended',
]

const DEFAULTS = {
  pollInterval: 45000, pollJitter: 10000, shardItemSlot: 1, kingSmpSlot: 24,
  afkGuiKeyword: 'KHU AFK', afkGuiSlots: 15, webPort: 3000,
}
const settings = { ...DEFAULTS, ...(config.settings || {}) }

class Bot extends EventEmitter {
  constructor(cfg, theme, proxyManager) {
    super()
    this.cfg = cfg; this.theme = theme; this.proxyManager = proxyManager
    this.proxy = null; this.mc = null
    this.packetMgr = new PacketManager(cfg.id)
    this.cmdRegistry = new CommandRegistry(this)
    this.customCommands = new Map()
    this.state = {
      connState: CS.DISCONNECTED, afk: null, intendedAfk: null,
      shard: 0, reconnects: 0, ping: -1, position: null,
      health: 20, food: 20, loginTime: null,
      inventory: [],
      tshard: false, autoStats: false, autoShard: false,
    }
    this._timers = new Map()
    this._disabled = false
    this._menuRetryCount = 0
    this._menuSuccess = false
    this._registerCommands()
  }

  log(level, msg) {
    const entry = { time: nowMs(), level, msg }
    getBotLog(this.cfg.id).push(entry)
    this.emit('log', { level, id: this.cfg.id, msg, theme: this.theme })
    // BUG 3 FIX: emit log only to subscribers of this bot's room, not broadcast globally
    if (io) io.to(`bot:${this.cfg.id}`).emit('log', entry)
    // NOTE: log_broadcast removed — was flooding all socket clients regardless of subscription
  }

  _setTimer(key, fn, delay, repeat = false) {
    this._clearTimer(key)
    let handle
    if (repeat) {
      handle = setInterval(fn, delay)
    } else {
      handle = setTimeout(() => { this._timers.delete(key); fn() }, delay)
    }
    this._timers.set(key, { handle, repeat })
    return handle
  }
  _clearTimer(key) {
    const t = this._timers.get(key)
    if (!t) return
    t.repeat ? clearInterval(t.handle) : clearTimeout(t.handle)
    this._timers.delete(key)
  }
  _clearAllTimers() { for (const [key] of this._timers) this._clearTimer(key) }

  _setState(newState) {
    const prev = this.state.connState
    if (prev === newState) return
    this.state.connState = newState
    this.emit('stateChange', { prev, now: newState })
    if (io) io.emit('botState', { id: this.cfg.id, state: newState })
  }

  get isOnline() { return this.state.connState === CS.ONLINE }
  get isConnected() { return [CS.ONLINE, CS.SPAWNING, CS.AUTHENTICATING].includes(this.state.connState) }
  get isStopping() { return this.state.connState === CS.STOPPING }
  get isReconnecting() { return this.state.connState === CS.RECONNECTING }

  requireOnline(cmdName = '') {
    if (!this.isOnline) {
      this.log('warn', `Bot chưa ONLINE${cmdName ? ' — lệnh "' + cmdName + '" bị bỏ qua' : ''}`)
      return false
    }
    return true
  }

  _startAutoStats() {
    this._clearTimer('autoStatsLoop')
    const loop = () => {
      if (!this.isOnline || !this.state.autoStats) return
      try {
        this.mc.chat('/stats')
      } catch (e) { this.log('err', 'Auto stats lỗi: ' + e.message) }
      this._setTimer('autoStatsLoop', loop, 60000)
    }
    this._setTimer('autoStatsLoop', loop, 5000)
  }

  _startAutoShard() {
    this._clearTimer('autoShardLoop')
    const loop = () => {
      if (!this.isOnline || !this.state.autoShard) return
      try {
        const n = this.readShard()
        if (n !== null) this._updateShard(n)
      } catch (e) { this.log('err', 'Auto shard lỗi: ' + e.message) }
      this._setTimer('autoShardLoop', loop, 30000)
    }
    this._setTimer('autoShardLoop', loop, 3000)
  }

  _resumeIntendedStates() {
    if (!this.isOnline) return
    
    if (this.state.intendedAfk) {
      this._setTimer('resumeAfk', () => {
        if (!this.isOnline) return
        if (this.state.intendedAfk === 'jump') this.afkJump()
        else if (this.state.intendedAfk === 'walk') this.afkWalk()
        this.log('sys', `Đã tự động khôi phục AFK: ${this.state.intendedAfk}`)
      }, 3000)
    }

    if (this.state.tshard) {
      this._setTimer('resumeTshard', () => {
        if (!this.isOnline || !this.state.tshard) return
        try {
          this.mc.chat('/afk')
          this.log('sys', 'Tự động gửi /afk (Treo Shard)')
        } catch (e) { this.log('err', 'Lỗi tự động Treo Shard: ' + e.message) }
      }, 5000)
    }

    if (this.state.autoStats) {
      this._startAutoStats()
    }

    if (this.state.autoShard) {
      this._startAutoShard()
    }
  }

  broadcastStatus() {
    if (io && typeof manager !== 'undefined' && manager.dashboard) {
      const snap = JSON.stringify(manager.bots.map(bot => manager.dashboard._botSummary(bot)))
      manager.dashboard._prevStatusSnap = snap
      io.emit('statusUpdate', { bots: JSON.parse(snap) })
    }
  }

  _updateShard(n) {
    if (n === null || n === this.state.shard) return
    const prev = this.state.shard
    this.state.shard = n
    this.emit('shard', { prev, now: n })
    if (io) io.emit('shard', { id: this.cfg.id, shard: n })
  }

  _updateInventory() {
    if (!this.mc?.inventory) return
    try {
      const inv = []
      const items = this.mc.inventory.items()
      for (const item of items) {
        if (!item) continue
        inv.push({
          slot: item.slot,
          name: resolveText(item.customName || item.displayName || item.name || 'Unknown'),
          type: item.name || item.type,
          count: item.count,
          lore: item.customLore ? safeJsonStringify(item.customLore).substring(0, 200) : '',
          stackId: item.stackId,
        })
      }
      this.state.inventory = inv
      if (io) io.emit('inventory', { id: this.cfg.id, items: inv })
    } catch (e) { this.log('err', 'Lỗi đọc inventory: ' + e.message) }
  }

  _cleanupOnDisconnect() {
    this._clearAllTimers(); this.packetMgr.detach()
    this.state.afk = null; this.state.ping = -1; this.state.loginTime = null
  }

  _destroyMc() {
    if (!this.mc) return
    const mc = this.mc; this.mc = null
    try { const c = mc._client; if (c && !c.ended) { c.end() } } catch { }
    try { mc.end('cleanup') } catch { }
    try { mc.removeAllListeners() } catch { }
    try { const c = mc._client; if (c) c.removeAllListeners() } catch { }
  }

  scheduleReconnect(reason) {
    if (this._disabled || this.isStopping || this.isReconnecting) return
    this._cleanupOnDisconnect(); this._destroyMc()
    this._setState(CS.RECONNECTING)
    if (this.state.reconnects >= TIMING.MAX_RECONNECT) {
      this.log('err', `Đã đạt giới hạn ${TIMING.MAX_RECONNECT} reconnect — thử lại sau 15 phút`)
      this._setState(CS.DISCONNECTED)
      this._setTimer('reconnect_longwait', () => {
        if (this._disabled) return
        this.log('warn', 'Thử kết nối lại sau giới hạn reconnect...')
        this.state.reconnects = 0
        this.start()
      }, 15 * 60 * 1000)
      return
    }
    const baseDelay = TIMING.RECONNECT_DELAYS[Math.min(this.state.reconnects, TIMING.RECONNECT_DELAYS.length - 1)]
    const delay = jit(baseDelay, Math.round(baseDelay * 0.3))
    this.state.reconnects++
    this.log('warn', `Mất kết nối${reason ? ' (' + reason + ')' : ''} — thử lại lần ${this.state.reconnects}/${TIMING.MAX_RECONNECT} sau ${(delay / 1000).toFixed(1)}s`)
    this._setTimer('reconnect', () => this.start(), delay)
  }

  cancelReconnect() {
    this._clearTimer('reconnect')
    if (this.isReconnecting) this._setState(CS.DISCONNECTED)
  }

  forceReconnect() {
    this.log('sys', 'Force reconnect...')
    this.cancelReconnect(); this.state.reconnects = 0
    this._cleanupOnDisconnect(); this._destroyMc()
    this._setState(CS.DISCONNECTED)
    this._setTimer('reconnect', () => this.start(), 500)
  }

  async start() {
    if (this._disabled || this.isStopping) return
    if (this.isConnected) { this.log('warn', 'start() gọi khi bot đã kết nối — bỏ qua'); return }
    this._destroyMc(); this._clearAllTimers()
    this._menuRetryCount = 0
    this._menuSuccess = false
    this._setState(CS.CONNECTING)
    const { cfg } = this
    const proxy = this.proxy || (cfg.useProxy !== false ? this.proxyManager.next() : null)
    let proxySocket = null
    if (proxy) {
      this.log('proxy', `Kết nối qua ${proxy.type}://${proxy.host}:${proxy.port}`)
      try { proxySocket = await this.proxyManager.connect(proxy, cfg.host, cfg.port); proxySocket.on('error', () => { }) }
      catch (e) { this.log('err', 'Proxy lỗi: ' + e.message + ' — thử kết nối thẳng'); proxySocket = null }
    }
    const botOpts = {
      host: cfg.host, port: cfg.port, username: cfg.username,
      version: cfg.version || config.version, respawn: false,
      hideErrors: true, ...(proxySocket ? { stream: proxySocket } : {}),
      viewDistance: 'tiny',
      disabledPlugins: ['particles', 'sound', 'book', 'boss_bar', 'title', 'villager', 'scoreboard_fixed'],
    }
    let mc
    try { mc = mineflayer.createBot(botOpts) }
    catch (e) {
      this.log('err', 'Không tạo được bot: ' + e.message)
      this._setState(CS.DISCONNECTED); this.scheduleReconnect('createBot failed'); return
    }
    this.mc = mc; this.packetMgr.attach(mc); this._bindEvents(mc, proxy)
  }

  _bindEvents(mc, proxy) {
    const { cfg, state: s } = this
    mc.setMaxListeners(30)
    if (mc._client) mc._client.setMaxListeners(30)

    mc.once('login', () => {
      this._setState(CS.AUTHENTICATING); s.loginTime = nowMs()
      this.log('ok', `Đã đăng nhập → ${cfg.host}:${cfg.port}` + (proxy ? ` [${proxy.type}://${proxy.host}:${proxy.port}]` : ''))
      if (cfg.clientSettings && mc._client) {
        try { mc._client.write('settings', cfg.clientSettings); this.log('sys', 'Đã áp dụng client settings') }
        catch (e) { this.log('err', 'Lỗi settings: ' + e.message) }
      }
      this._setTimer('loginCmd', () => {
        if (!this.isConnected) return
        try {
          if (cfg.registered === false) { mc.chat(`/dk ${cfg.botPassword}`); cfg.registered = true; this.log('ok', 'Đã gửi /dk'); saveConfigDebounced() }
          else if (cfg.botPassword) { mc.chat(`/dn ${cfg.botPassword}`); this.log('ok', 'Đã gửi /dn') }
        } catch (e) { this.log('err', 'Lỗi gửi auth cmd: ' + e.message) }
      }, rand(1800, 3500))
    })

    let firstSpawn = true
    mc.on('spawn', () => {
      if (firstSpawn) {
        firstSpawn = false; this._setState(CS.ONLINE); s.reconnects = 0
        this.log('ok', 'Spawn thành công')
        this._setTimer('stable', () => { if (this.isOnline) this.log('sys', 'Kết nối ổn định') }, TIMING.STABLE_TIME)
        this._setTimer('poll', () => this._pollTick(), jit(settings.pollInterval, settings.pollJitter / 2))
        this._startHealthCheck()
        this._setTimer('invUpdate', () => this._updateInventory(), 3000)

        if (cfg.autoMenu && cfg.menuCommand) {
          this._menuRetryCount = 0
          this._menuSuccess = false
          this._scheduleMenuRetry()
        } else {
          this._resumeIntendedStates()
        }
      } else {
        this.log('sys', 'Đã hồi sinh (respawn)')
        this._resumeIntendedStates()
      }
    })

    mc.on('death', () => {
      const d = rand(2500, 8000)
      this.log('warn', `Chết — respawn sau ${d}ms`)
      this._setTimer('respawn', () => { if (this.isOnline) try { mc.respawn() } catch { } }, d)
    })

    mc.on('ping', p => {
      s.ping = typeof p === 'number' ? p : -1
      if (io) io.emit('ping', { id: cfg.id, ping: s.ping })
    })

    mc.on('move', () => { if (mc.entity?.position) s.position = { ...mc.entity.position } })

    mc.on('health', () => {
      s.health = mc.health ?? 20; s.food = mc.food ?? 20
      if (io) io.emit('health', { id: cfg.id, health: s.health, food: s.food })
    })

    const scheduleInvUpdate = (delay) => this._setTimer('invDebounce', () => this._updateInventory(), delay)
    mc.on('playerCollect', () => scheduleInvUpdate(500))
    mc.on('windowClose', () => scheduleInvUpdate(300))
    mc.on('setSlot', () => scheduleInvUpdate(200))

    mc.on('message', (json, pos) => {
      try {
        const text = resolveText(json?.json ?? json)
        if (!text.trim()) return
        if (pos === 'game_info' || pos === 'action_bar') { this.tryChatShard(text); return }
        this.log('chat', text); this.tryChatShard(text)
        // BUG 3 FIX: chat already goes to bot room via this.log() above
        // no need for extra io.to() here — removed duplicate emit
      } catch (e) { this.log('err', 'Lỗi message: ' + e.message) }
    })

    mc.on('messagestr', (msg, pos) => {
      try { if (pos === 'gameInfo') this.tryChatShard(msg) } catch { }
    })

    mc.on('scoreboardUpdated', () => { try { this._updateShard(this.readShard()) } catch { } })

    mc.on('windowOpen', win => {
      this._setTimer('winOpen_' + win.id, () => {
        if (!this.isOnline) return
        try {
          const title = resolveText(win.title || '').toUpperCase()
          if (cfg.autoMenu && cfg.menuCommand && !this._menuSuccess) {
            if (title.includes('MENU') || title.includes('LOBBY') || title.includes('HUB') ||
                title.includes('CHỌN') || title.includes('KHU') || title.includes('WORLD')) {
              this._menuSuccess = true
              this._clearTimer('menuRetry')
              this.log('ok', `Đã vào server thành công qua menu: [${title.substring(0, 40)}]`)
              WindowRouter.route(this, win)
              this._resumeIntendedStates()
              return
            }
          }
          WindowRouter.route(this, win)
        }
        catch (e) { this.log('err', 'Lỗi windowOpen: ' + e.message) }
      }, jit(2000, 600))
    })

    mc.on('error', err => {
      const m = err?.message || String(err)
      if (IGNORED_ERRORS.some(k => m.includes(k))) return
      this.log('err', m)
    })
    mc.on('kicked', reason => {
      const m = typeof reason === 'string' ? stripMc(reason) : resolveText(reason)
      this.log('warn', 'Bị kick: ' + m)
    })
    mc.once('end', reason => {
      const m = typeof reason === 'string' ? reason : resolveText(reason)
      if (this.isStopping || this.isReconnecting) return
      this.scheduleReconnect(m || 'connection ended')
    })
  }

  _scheduleMenuRetry() {
    const MAX_MENU_RETRIES = 999
    const BASE_DELAY = 8000
    const retry = () => {
      if (!this.isOnline || this._menuSuccess || this._disabled) return
      if (this._menuRetryCount >= MAX_MENU_RETRIES) {
        this.log('warn', `Đã thử menu ${MAX_MENU_RETRIES} lần — dừng`)
        return
      }
      this._menuRetryCount++
      const delay = this._menuRetryCount <= 3 ? BASE_DELAY : jit(BASE_DELAY * 2, 3000)
      this.log('sys', `Gửi menu lần ${this._menuRetryCount}: ${this.cfg.menuCommand}`)
      try { this.mc.chat(this.cfg.menuCommand) } catch (e) { this.log('err', 'Lỗi gửi menu: ' + e.message) }
      if (!this._menuSuccess) {
        this._setTimer('menuRetry', retry, delay)
      }
    }
    this._setTimer('menuRetry', retry, rand(5000, 8000))
  }

  _startHealthCheck() {
    this._clearTimer('health')
    const check = () => {
      if (!this.isOnline) return
      const mc = this.mc
      try {
        const client = mc?._client
        const socketAlive = client && !client.ended && (
          (client.socket && !client.socket.destroyed) ||
          (client.stream && !client.stream.destroyed)
        )
        if (!socketAlive) { this.log('health', 'Socket chết — đang reconnect'); this.scheduleReconnect('socket dead'); return }
        if (this.packetMgr.isStale(TIMING.PACKET_TIMEOUT)) {
          this.log('health', `Không nhận packet trong ${TIMING.PACKET_TIMEOUT / 1000}s — reconnect`)
          this.scheduleReconnect('packet timeout'); return
        }
        if (!mc?.entity) {
          const onlineFor = this.state.loginTime ? nowMs() - this.state.loginTime : 0
          if (onlineFor > TIMING.ENTITY_TIMEOUT) {
            this.log('health', 'Entity null sau spawn — reconnect'); this.scheduleReconnect('entity null'); return
          }
        }
      } catch (e) { this.log('err', 'Lỗi health check: ' + e.message) }
    }
    this._setTimer('health', check, jit(TIMING.HEALTH_INTERVAL, TIMING.HEALTH_JITTER), true)
  }

  _pollTick() {
    if (!this.isOnline) return
    this._updateShard(this.readShard())
    this._updateInventory()
    this._setTimer('poll', () => this._pollTick(), jit(settings.pollInterval, settings.pollJitter))
  }

  shutdown() {
    this._disabled = true; this._setState(CS.STOPPING)
    this._cleanupOnDisconnect()
    try { this.afkStop() } catch { }
    this._destroyMc(); this.log('sys', 'Bot đã tắt')
  }

  // BUG 2 FIX: expanded fallback chain for shard parsing
  // Order: displayName → lore text → count → nbt raw
  readShard() {
    const mc = this.mc
    if (!mc?.scoreboards) return null
    try {
      for (const name in mc.scoreboards) {
        const sb = mc.scoreboards[name]
        if (!sb?.itemsMap) continue
        for (const entry in sb.itemsMap) {
          let parts = [entry]
          if (mc.teamMap) {
            const team = Object.values(mc.teamMap).find(t => t.members?.includes(entry))
            if (team) parts = [resolveText(team.prefix), entry, resolveText(team.suffix)]
          }
          if (sb.itemsMap[entry]?.displayName) parts.push(resolveText(sb.itemsMap[entry].displayName))
          const n = parseShardNum(parts.join(' '))
          if (n !== null) { this.log('shard', 'Scoreboard → ' + n.toLocaleString()); return n }
        }
      }
    } catch (e) { this.log('err', 'Lỗi scoreboard: ' + e.message) }
    return null
  }

  tryChatShard(raw) {
    try {
      const text = typeof raw === 'string' ? stripMc(raw) : resolveText(raw)
      const n = parseShardNum(text)
      if (n !== null) this._updateShard(n)
    } catch { }
  }

  _registerCommands() {
    const r = this.cmdRegistry
    r.register('shard', 'Bật/Tắt Auto Shard', () => {
      if (!this.requireOnline('shard')) return
      this.state.autoShard = !this.state.autoShard
      this.log('sys', `Tự động đọc Shard: ${this.state.autoShard ? 'BẬT' : 'TẮT'}`)
      if (this.state.autoShard) {
        this._startAutoShard()
      } else {
        this._clearTimer('autoShardLoop')
      }
      this.broadcastStatus()
    })
    r.register('stats', 'Bật/Tắt Auto Stats', () => {
      if (!this.requireOnline('stats')) return
      this.state.autoStats = !this.state.autoStats
      this.log('sys', `Tự động stats: ${this.state.autoStats ? 'BẬT' : 'TẮT'}`)
      if (this.state.autoStats) {
        this._startAutoStats()
      } else {
        this._clearTimer('autoStatsLoop')
      }
      this.broadcastStatus()
    })
    r.register('tshard', 'Bật/Tắt Treo Shard', () => {
      if (!this.requireOnline('tshard')) return
      this.state.tshard = !this.state.tshard
      this.log('sys', `Treo Shard: ${this.state.tshard ? 'BẬT' : 'TẮT'}`)
      if (this.state.tshard) {
        try {
          this.mc.chat('/afk')
          this.log('sys', 'Đã gửi /afk')
        } catch (e) { this.log('err', e.message) }
      }
      this.broadcastStatus()
    })
    r.register('afk', 'Bật/Tắt AFK jump', () => {
      if (!this.requireOnline('afk')) return
      if (this.state.afk === 'jump') {
        this.afkStop()
      } else {
        this.afkJump()
      }
      this.broadcastStatus()
    })
    r.register('wafk', 'Bật/Tắt AFK walk', () => {
      if (!this.requireOnline('wafk')) return
      if (this.state.afk === 'walk') {
        this.afkStop()
      } else {
        this.afkWalk()
      }
      this.broadcastStatus()
    })
    r.register('automenu', 'Bật/Tắt Tự động Menu', () => {
      this.cfg.autoMenu = !this.cfg.autoMenu
      this.log('sys', `Tự động Menu: ${this.cfg.autoMenu ? 'BẬT' : 'TẮT'}`)
      saveConfigDebounced()
      this.broadcastStatus()
    })
    r.register('stop', 'Dừng AFK', () => {
      this.afkStop()
      this.broadcastStatus()
    })
    r.register('tpa', 'TPA tới owner', () => {
      if (!this.requireOnline('tpa')) return
      try {
        this.mc.chat(`/tpa ${this.cfg.ownerUsername}`)
        this.log('sys', `TPA → ${this.cfg.ownerUsername}`)
      } catch (e) { this.log('err', e.message) }
    })
    r.register('ping', 'Hiện ping', () => {
      this.log('sys', `Ping: ${this.state.ping >= 0 ? this.state.ping + 'ms' : 'N/A'}`)
    })
    r.register('pos', 'Hiện tọa độ', () => {
      const p = this.state.position
      if (!p) { this.log('warn', 'Chưa có tọa độ'); return }
      this.log('sys', `Vị trí: X=${p.x?.toFixed(2)} Y=${p.y?.toFixed(2)} Z=${p.z?.toFixed(2)}`)
    })
    r.register('inv', 'Xem inventory', () => {
      this._updateInventory()
      const inv = this.state.inventory
      if (!inv.length) { this.log('warn', 'Túi đồ trống'); return }
      inv.forEach(item => this.log('sys', `[${item.slot}] ${item.name} x${item.count}`))
    })
    r.register('status', 'Hiện trạng thái', () => this.emit('status'))
    r.register('reconnect', 'Force reconnect', () => this.forceReconnect())
    r.register('menu', 'Gửi menu command thủ công', () => {
      if (!this.requireOnline('menu')) return
      if (!this.cfg.menuCommand) { this.log('warn', 'Chưa có menuCommand'); return }
      try { this.mc.chat(this.cfg.menuCommand); this.log('sys', `Gửi menu: ${this.cfg.menuCommand}`) }
      catch (e) { this.log('err', e.message) }
    })
    r.register('addcmd', 'Thêm custom command: addcmd <tên> <lệnh MC>', (args) => {
      if (args.length < 2) { this.log('warn', 'Cú pháp: addcmd <tên> <lệnh>'); return }
      const name = args[0].toLowerCase()
      const cmd = args.slice(1).join(' ')
      if (this.cmdRegistry._commands.has(name)) {
        this.log('warn', `Tên "${name}" trùng lệnh hệ thống — custom cmd sẽ được ưu tiên`)
      }
      this.customCommands.set(name, cmd)
      this.log('ok', `Đã thêm lệnh "${name}" → "${cmd}"`)
      if (io) io.emit('customCmds', { id: this.cfg.id, cmds: this.getCustomCmds() })
    })
    r.register('delcmd', 'Xóa custom command: delcmd <tên>', (args) => {
      if (!args[0]) { this.log('warn', 'Cú pháp: delcmd <tên>'); return }
      const name = args[0].toLowerCase()
      if (this.customCommands.delete(name)) {
        this.log('ok', `Đã xóa lệnh "${name}"`)
        if (io) io.emit('customCmds', { id: this.cfg.id, cmds: this.getCustomCmds() })
      } else { this.log('warn', `Không tìm thấy lệnh "${name}"`) }
    })
    r.register('listcmd', 'Xem danh sách custom commands', () => {
      if (!this.customCommands.size) { this.log('sys', 'Chưa có custom command nào'); return }
      for (const [name, cmd] of this.customCommands) this.log('sys', `  ${name} → ${cmd}`)
    })
  }

  getCustomCmds() {
    const out = []
    for (const [name, cmd] of this.customCommands) out.push({ name, cmd })
    return out
  }

  cmd(input) {
    const trimmed = String(input).trim()
    const parts = trimmed.split(/\s+/)
    const key = parts[0].toLowerCase()
    if (this.customCommands.has(key)) {
      if (!this.isOnline) { this.log('warn', 'Bot offline — không gửi được chat'); return }
      try {
        const c = this.customCommands.get(key)
        this.mc.chat(c.startsWith('/') ? c : `/${c}`)
        this.log('sys', `Custom cmd "${key}" → ${c}`)
      } catch (e) { this.log('err', 'Lỗi custom cmd: ' + e.message) }
      return
    }
    if (!this.cmdRegistry.run(trimmed)) {
      if (!this.isOnline) { this.log('warn', 'Bot offline — không gửi được chat'); return }
      try { this.mc.chat(trimmed.startsWith('/') ? trimmed : `/${trimmed}`) }
      catch (e) { this.log('err', 'Lỗi gửi chat: ' + e.message) }
    }
  }
}

const WindowRouter = {
  _routes: [],
  register(name, matcher, handler) { this._routes.push({ name, matcher, handler }) },
  route(bot, win) {
    const title = resolveText(win.title || '').toUpperCase()
    for (const route of this._routes) {
      if (route.matcher(title, win)) { route.handler(bot, win); return }
    }
    this._handleUnknown(bot, win, title)
  },
  // BUG 1 FIX: set menuSuccess=true whenever we click any slot in an unknown menu window
  // Previously _handleUnknown never set menuSuccess, causing infinite menu retry loop
  _handleUnknown(bot, win, title) {
    bot.parseWindowShard(win)
    const slot = settings.kingSmpSlot
    if (win.slots?.length > slot && win.slots[slot]) {
      const isMenu = /menu|kingsmp|mở rộng|gui/i.test(title)
      if (isMenu || bot.cfg.autoMenu) {
        bot._menuSuccess = true  // BUG 1 FIX: mark success so menu retry loop stops
        bot.mc.clickWindow(slot, 0, 0)
        bot.log('sys', `Click slot ${slot} trong [${title}]`)
        bot._resumeIntendedStates()
      } else { bot.mc.closeWindow(win) }
    } else { bot.mc.closeWindow(win) }
  },
}

WindowRouter.register('STATS_WINDOW', t => t.includes('STATS') || t.includes('THỐNG KÊ'), (bot, win) => StatsParser.parse(bot, win))
WindowRouter.register('AFK_WINDOW', t => t.includes(settings.afkGuiKeyword.toUpperCase()), (bot, win) => AfkGuiHandler.handle(bot, win))
WindowRouter.register('SHARD_WINDOW', t => t.includes('SHARD') || t.includes('MẢNH'), (bot, win) => bot.parseWindowShard(win))
WindowRouter.register('TPA_WINDOW', t => t.includes('/TPA') || t.includes('TPA'), (bot, win) => {
  bot._setTimer('tpaConfirm', () => {
    if (!bot.isOnline) return
    try {
      let foundSlot = -1
      for (let i = 0; i < win.slots.length; i++) {
        const sl = win.slots[i]
        if (sl) {
          const name = (sl.name || '').toLowerCase()
          const disp = resolveText(sl.customName || sl.displayName || '').toLowerCase()
          if (name.includes('green_stained_glass_pane') || 
              (name.includes('glass') && name.includes('green')) ||
              disp.includes('xanh lá') || 
              disp.includes('green stained glass')) {
            foundSlot = i
            break
          }
        }
      }
      if (foundSlot !== -1) {
        const sl = win.slots[foundSlot]
        const slotName = sl ? resolveText(sl.customName || sl.displayName || sl.name || '') : '(trống)'
        bot.mc.clickWindow(foundSlot, 0, 0)
        bot.log('ok', `GUI TPA: Đã click slot ${foundSlot + 1} (ô thứ ${foundSlot + 1}): "${slotName}" — Đồng ý TPA`)
      } else {
        bot.log('warn', 'GUI TPA: Không tìm thấy kính màu xanh lá để click')
        bot.mc.closeWindow(win)
      }
    } catch (e) { bot.log('err', 'Lỗi xử lý GUI TPA: ' + e.message) }
  }, jit(1800, 500))
})

// BUG 2 FIX: expanded fallback chain for window shard parsing
// Chain: customName/displayName with SHARD keyword → lore text (expanded regex) → nbt raw → item count
Bot.prototype.parseWindowShard = function (win) {
  try {
    let item = null
    for (const sl of win.slots) {
      if (!sl) continue
      if (resolveText(sl.customName || sl.displayName || '').toUpperCase().includes('SHARD')) { item = sl; break }
    }
    item = item || win.slots[settings.shardItemSlot]
    if (!item) { this.log('warn', 'Không có item shard trong window'); return }

    let n = 0

    // Fallback 1: lore text — expanded regex to catch more server formats
    if (item.customLore && !n) {
      const loreStr = safeJsonStringify(item.customLore)
      // Try strict structured format first
      let m = loreStr.match(/"text":\{"type":"string","value":"([^"]*\d[^"]*)"\}/)
      if (m) n = parseInt(m[1].replace(/[^\d]/g, ''), 10)
      // Fallback: any quoted string with 3+ digit number
      if (!n) { m = loreStr.match(/(\d{3,})/); if (m) n = parseInt(m[1], 10) }
      // Fallback: plain "text":"<value>" format used by some servers
      if (!n) {
        m = loreStr.match(/"text"\s*:\s*"([^"]*)"/)
        if (m) { const p = parseShardNum(m[1]); if (p !== null) n = p }
      }
    }

    // Fallback 2: nbt raw
    if (!n && item.nbt) {
      const s = safeJsonStringify(item.nbt)
      const m = s.match(/(\d{4,})/); if (m) n = parseInt(m[1], 10)
    }

    // Fallback 3: displayName / customName text
    if (!n) {
      const texts = [resolveText(item.customName || item.displayName || '')]
      if (item.customLore) texts.push(safeJsonStringify(item.customLore))
      const p = parseShardNum(texts.join(' '))
      if (p !== null) n = p
    }

    // Fallback 4: raw displayName string with any number
    if (!n && item.displayName) { const m = String(item.displayName).match(/(\d+)/); if (m) n = parseInt(m[1], 10) }

    // Fallback 5: item count as last resort
    if (!n && item.count && item.count > 1) n = item.count

    if (n > 0) this._updateShard(n)
    else this.log('warn', `Không parse được shard từ window`)
  } catch (e) { this.log('err', 'Lỗi parseWindowShard: ' + e.message) }
}

const StatsParser = {
  _extractNum(sl) {
    if (!sl) return 0
    if (sl.customLore) {
      try {
        const s = safeJsonStringify(sl.customLore)
        const m = s.match(/"text":\{"type":"string","value":"([^"]*\d[^"]*)"\}/)
        if (m) { const n = parseInt(m[1].replace(/[^\d]/g, ''), 10); if (n > 0) return n }
        const m2 = s.match(/(\d{3,})/); if (m2) { const n = parseInt(m2[1], 10); if (n > 0) return n }
        // BUG 2 FIX: additional plain "text" format fallback
        const m3 = s.match(/"text"\s*:\s*"([^"]*)"/)
        if (m3) { const p = parseShardNum(m3[1]); if (p !== null && p > 0) return p }
      } catch { }
    }
    if (sl.nbt) {
      try { const s = safeJsonStringify(sl.nbt); const m = s.match(/(\d{4,})/); if (m) { const n = parseInt(m[1], 10); if (n > 0) return n } } catch { }
    }
    const nameText = resolveText(sl.customName || sl.displayName || '')
    const p = parseShardNum(nameText); if (p !== null && p > 0) return p
    return sl.count || 0
  },

  parse(bot, win) {
    try {
      let shard = 0, money = 0
      let amethystItem = null, emeraldItem = null
      for (const sl of win.slots) {
        if (!sl) continue
        const type = (sl.type || '').toLowerCase()
        const name = resolveText(sl.customName || sl.displayName || '').toLowerCase()
        if (!amethystItem && (type.includes('amethyst') || name.includes('amethyst'))) { amethystItem = sl; shard = this._extractNum(sl) }
        if (!emeraldItem && (type.includes('emerald') || name.includes('emerald'))) { emeraldItem = sl; money = this._extractNum(sl) }
      }
      const entity = bot.mc?.entity
      const health = entity ? Math.ceil((entity.health || 20) / 2) : 0
      const pos = entity?.position
      const x = pos?.x?.toFixed(2) ?? '?'; const y = pos?.y?.toFixed(2) ?? '?'; const z = pos?.z?.toFixed(2) ?? '?'
      const elapsed = bot.state.loginTime ? Math.floor((nowMs() - bot.state.loginTime) / 1000) : 0
      const hours = Math.floor(elapsed / 3600); const minutes = Math.floor((elapsed % 3600) / 60); const seconds = elapsed % 60
      const info = [
        `${chalk.dim.rgb(130, 130, 175)('Shard  ')}  ${chalk.rgb(0, 225, 150).bold(shard.toLocaleString())}`,
        `${chalk.dim.rgb(130, 130, 175)('Money  ')}  ${chalk.rgb(0, 225, 100).bold(money.toLocaleString())}`,
        `${chalk.dim.rgb(130, 130, 175)('Health ')}  ${chalk.rgb(215, 65, 65).bold(health)} ❤`,
        `${chalk.dim.rgb(130, 130, 175)('Time   ')}  ${chalk.rgb(255, 200, 0).bold(`${hours}h ${minutes}m ${seconds}s`)}`,
        `${chalk.dim.rgb(130, 130, 175)('Pos    ')}  ${chalk.rgb(120, 185, 255).bold(`X:${x} Y:${y} Z:${z}`)}`,
      ]
      bot.emit('log', { level: 'sys', id: bot.cfg.id, msg: null, box: createRoundBox(info.join('\n'), chalk.rgb(...bot.theme.border), `STATS  ${bot.cfg.id}`) })
      if (shard > 0) bot._updateShard(shard)
    } catch (e) { bot.log('err', 'Lỗi StatsParser: ' + e.message) }
  },
}

const AfkGuiHandler = {
  handle(bot, win) {
    try {
      const max = Math.min(settings.afkGuiSlots, win.slots.length)
      for (let i = 0; i < max; i++) {
        const sl = win.slots[i]
        if (!sl || AfkGuiHandler._isFull(sl)) { if (sl) bot.log('warn', `Khu AFK #${i + 1} đầy`); continue }
        bot.mc.clickWindow(i, 0, 0); bot.log('ok', `Đã chọn khu AFK #${i + 1}`); return
      }
      bot.log('warn', 'Tất cả khu AFK đầy — đóng GUI'); bot.mc.closeWindow(win)
    } catch (e) { bot.log('err', 'Lỗi AfkGuiHandler: ' + e.message) }
  },
  _isFull(sl) {
    const pieces = [resolveText(sl.customName || sl.displayName || '')]
    if (sl.customLore) try { pieces.push(safeJsonStringify(sl.customLore)) } catch { }
    if (sl.nbt) try { pieces.push(safeJsonStringify(sl.nbt)) } catch { }
    const text = pieces.join(' ')
    if (/ĐẦY|HẾT\s*CHỖ|FULL|MAX(?:IMUM)?|KHÓA|KHÔNG\s*THỂ|ĐANG\s*ĐẦY|ĐÃ\s*ĐẦY|QUÁ\s*TẢI/i.test(text)) return true
    const m = text.match(/(\d+)\s*\/\s*(\d+)/)
    return !!(m && parseInt(m[1], 10) >= parseInt(m[2], 10))
  },
}

Bot.prototype.afkJump = function () {
  this._clearTimer('afk'); this._clearTimer('wafk')
  this.state.afk = 'jump'; this.state.intendedAfk = 'jump'
  const mc = this.mc
  const tick = () => {
    if (!this.isOnline || this.state.afk !== 'jump') return
    try {
      mc.setControlState('jump', true)
      this._setTimer('afkJumpOff', () => { try { if (this.isOnline) mc.setControlState('jump', false) } catch { } }, rand(100, 450))
      if (Math.random() < 0.30) { const yaw = (mc.entity?.yaw || 0) + (Math.random() - 0.5) * 1.0; const pitch = (mc.entity?.pitch || 0) + (Math.random() - 0.5) * 0.4; mc.look(yaw, clamp(pitch, -1.4, 1.4), true) }
      if (Math.random() < 0.06) mc.swingArm()
      if (Math.random() < 0.03) { mc.setControlState('sneak', true); this._setTimer('afkSneak', () => { try { if (this.isOnline) mc.setControlState('sneak', false) } catch { } }, rand(300, 900)) }
      if (Math.random() < 0.02 && mc._client) { try { mc._client.write('tab_complete', { text: '/', assumeCommand: false }) } catch { } }
      if (Math.random() < 0.03) { try { mc.setQuickBarSlot(rand(0, 8)) } catch { } }
    } catch (e) { this.log('err', 'AFK jump lỗi: ' + e.message) }
    this._setTimer('afk', tick, jit(4800, 2000))
  }
  this._setTimer('afk', tick, jit(700, 200))
  this.log('afk', 'Jump AFK bật')
  if (io) io.emit('afk', { id: this.cfg.id, mode: 'jump' })
  this.broadcastStatus()
}

Bot.prototype.afkWalk = function () {
  this._clearTimer('afk'); this._clearTimer('wafk')
  this.state.afk = 'walk'; this.state.intendedAfk = 'walk'
  const mc = this.mc
  let yaw = mc.entity?.yaw || 0; let dir = 1; let step = 0; let lastPos = null; let stuckTicks = 0
  const tick = () => {
    if (!this.isOnline || this.state.afk !== 'walk') return
    try {
      if (Math.random() < 0.06) dir = -dir
      yaw += rand(2, 10) * 0.09 * dir
      mc.look(yaw, (Math.random() - 0.5) * 0.25, true)
      if (++step > rand(8, 20)) {
        step = 0
        const keys = ['forward', 'back', 'left', 'right']; const k = keys[rand(0, 3)]
        mc.setControlState(k, true)
        this._setTimer('wafkKey', () => { try { if (this.isOnline) mc.setControlState(k, false) } catch { } }, rand(200, 950))
      }
      if (Math.random() < 0.04) { mc.setControlState('jump', true); this._setTimer('wafkJump', () => { try { if (this.isOnline) mc.setControlState('jump', false) } catch { } }, rand(100, 300)) }
      if (Math.random() < 0.05) mc.swingArm()
      if (Math.random() < 0.02 && mc._client) { try { mc._client.write('tab_complete', { text: '/', assumeCommand: false }) } catch { } }
      if (Math.random() < 0.03) { try { mc.setQuickBarSlot(rand(0, 8)) } catch { } }
      const currPos = mc.entity?.position
      if (lastPos && currPos) {
        const dist = Math.sqrt(Math.pow(currPos.x - lastPos.x, 2) + Math.pow(currPos.y - lastPos.y, 2) + Math.pow(currPos.z - lastPos.z, 2))
        if (dist < 0.1) { stuckTicks++; if (stuckTicks > 4) { dir = -dir; yaw += Math.PI; stuckTicks = 0 } } else { stuckTicks = 0 }
      }
      if (currPos) lastPos = { x: currPos.x, y: currPos.y, z: currPos.z }
    } catch (e) { this.log('err', 'AFK walk lỗi: ' + e.message) }
    this._setTimer('wafk', tick, jit(480, 150))
  }
  this._setTimer('wafk', tick, jit(400, 100))
  this.log('afk', 'Walk AFK bật')
  if (io) io.emit('afk', { id: this.cfg.id, mode: 'walk' })
  this.broadcastStatus()
}

Bot.prototype.afkStop = function () {
  this._clearTimer('afk'); this._clearTimer('wafk'); this._clearTimer('afkJumpOff')
  this._clearTimer('afkSneak'); this._clearTimer('wafkKey'); this._clearTimer('wafkJump'); this._clearTimer('resumeAfk')
  try {
    if (this.mc?.entity) for (const k of ['forward', 'back', 'left', 'right', 'jump', 'sneak', 'sprint']) this.mc.setControlState(k, false)
  } catch { }
  this.state.afk = null; this.state.intendedAfk = null
  this.log('sys', 'AFK đã dừng')
  if (io) io.emit('afk', { id: this.cfg.id, mode: null })
  this.broadcastStatus()
}

class CommandRegistry {
  constructor(bot) { this.bot = bot; this._commands = new Map() }
  register(name, desc, fn) { this._commands.set(name.toLowerCase(), { name, desc, fn }) }
  run(input) {
    const parts = input.trim().split(/\s+/)
    const key = parts[0].toLowerCase()
    const entry = this._commands.get(key)
    if (!entry) return false
    try { entry.fn(parts.slice(1)) } catch (e) { this.bot.log('err', `Lỗi lệnh "${key}": ${e.message}`) }
    return true
  }
  list() { return [...this._commands.values()].map(c => ({ name: c.name, desc: c.desc })) }
}

function getSystemMetrics() {
  const totalMem = os.totalmem()
  const freeMem = os.freemem()
  const usedMem = totalMem - freeMem
  const cpus = os.cpus()
  const loadAvg = os.loadavg()
  const uptime = os.uptime()
  const procMem = process.memoryUsage()
  return {
    totalMem, freeMem, usedMem,
    memPercent: Math.round((usedMem / totalMem) * 100),
    procHeap: procMem.heapUsed,
    procHeapTotal: procMem.heapTotal,
    procRss: procMem.rss,
    cpuCount: cpus.length,
    cpuModel: cpus[0]?.model || 'Unknown',
    loadAvg,
    uptime,
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
  }
}

function detectServerEnv() {
  const env = {
    isDocker: false, isTermux: false, isHeadless: !process.stdout.isTTY,
    nodeVersion: process.version, platform: process.platform,
    arch: process.arch, cpuCount: os.cpus().length,
    totalMem: Math.round(os.totalmem() / 1024 / 1024),
    autoExe: AUTO_EXE,
  }
  try {
    if (fs.existsSync('/.dockerenv')) env.isDocker = true
    const cgroup = fs.readFileSync('/proc/1/cgroup', 'utf8')
    if (cgroup.includes('docker') || cgroup.includes('containerd') || cgroup.includes('lxc')) env.isDocker = true
  } catch { }
  if (process.env.TERMUX_VERSION || process.env.PREFIX?.includes('com.termux') || fs.existsSync('/data/data/com.termux')) env.isTermux = true
  env.isLowResource = env.totalMem < 1024 || env.cpuCount <= 1
  return env
}

const SERVER_ENV = detectServerEnv()

if (SERVER_ENV.isLowResource) {
  settings.pollInterval = Math.max(settings.pollInterval, 90000)
  settings.pollJitter = Math.max(settings.pollJitter, 15000)
}

class WebDashboard {
  constructor(botList, proxyManager) {
    this.bots = botList
    this.proxyManager = proxyManager
    this.port = settings.webPort || 3000
    this._cachedHtml = ''
    // BUG 4 FIX: snapshot for dirty-flag diff — only emit statusUpdate when data actually changes
    this._prevStatusSnap = ''
  }

  start() {
    if (!expressApp || !io) {
      console.log('[Dashboard] express/socket.io không khả dụng')
      return
    }
    expressApp.use((req, res, next) => { res.setHeader('Access-Control-Allow-Origin', '*'); next() })
    expressApp.use(require('express').json())

    expressApp.get('/', (req, res) => res.send(this._cachedHtml))
    expressApp.get('/api/bots', (req, res) => res.json(this.bots.map(b => this._botSummary(b))))
    expressApp.get('/api/bots/:id', (req, res) => {
      const b = this._findBot(req.params.id)
      if (!b) return res.status(404).json({ error: 'Not found' })
      res.json({ ...this._botSummary(b), logs: getBotLog(b.cfg.id).toArray(), inventory: b.state.inventory, customCmds: b.getCustomCmds() })
    })
    expressApp.post('/api/bots/:id/cmd', (req, res) => {
      const b = this._findBot(req.params.id); if (!b) return res.status(404).json({ error: 'Not found' })
      const { cmd } = req.body || {}; if (!cmd) return res.status(400).json({ error: 'cmd required' })
      b.cmd(String(cmd)); res.json({ ok: true })
    })
    expressApp.post('/api/bots/:id/reconnect', (req, res) => {
      const b = this._findBot(req.params.id); if (!b) return res.status(404).json({ error: 'Not found' })
      b.forceReconnect(); res.json({ ok: true })
    })
    expressApp.post('/api/bots/:id/stop', (req, res) => {
      const b = this._findBot(req.params.id); if (!b) return res.status(404).json({ error: 'Not found' })
      b.shutdown(); res.json({ ok: true })
    })
    expressApp.delete('/api/bots/:id', (req, res) => {
      const b = this._findBot(req.params.id); if (!b) return res.status(404).json({ error: 'Not found' })
      b.shutdown()
      const idx = this.bots.indexOf(b)
      if (idx !== -1) this.bots.splice(idx, 1)
      botLogs.delete(b.cfg.id)
      if (io) io.emit('botRemoved', { id: b.cfg.id })
      res.json({ ok: true })
    })
    expressApp.patch('/api/bots/:id', (req, res) => {
      const b = this._findBot(req.params.id); if (!b) return res.status(404).json({ error: 'Not found' })
      const allowed = ['autoMenu', 'menuCommand', 'respawn', 'ownerUsername', 'botPassword', 'useProxy']
      for (const k of allowed) { if (req.body[k] !== undefined) b.cfg[k] = req.body[k] }
      const cfgBot = (config.bots || []).find(c => c.id === b.cfg.id)
      if (cfgBot) { for (const k of allowed) { if (req.body[k] !== undefined) cfgBot[k] = req.body[k] } }
      saveConfigDebounced()
      res.json({ ok: true, cfg: b.cfg })
    })
    expressApp.post('/api/bots', (req, res) => {
      const { id, host, port, username, password, version, proxyIdx } = req.body || {}
      if (!id || !host || !port || !username) return res.status(400).json({ error: 'id, host, port, username required' })
      if (this._findBot(id)) return res.status(409).json({ error: 'ID already exists' })
      const b = manager._createBotFromData({ id, host, port, username, password, version, proxyIdx })
      this.bots.push(b); b.start()
      io.emit('botAdded', this._botSummary(b))
      res.json({ ok: true, id: b.cfg.id })
    })
    expressApp.post('/api/bots/all/cmd', (req, res) => {
      const { cmd } = req.body || {}; if (!cmd) return res.status(400).json({ error: 'cmd required' })
      for (const b of this.bots) b.cmd(String(cmd))
      res.json({ ok: true, count: this.bots.length })
    })
    expressApp.get('/api/proxies', (req, res) => res.json(this.proxyManager.list))
    expressApp.post('/api/proxies', (req, res) => {
      const { proxy } = req.body || {}; if (!proxy) return res.status(400).json({ error: 'proxy required' })
      const r = this.proxyManager.add(proxy); res.json(r)
    })
    expressApp.delete('/api/proxies/:idx', (req, res) => {
      const idx = parseInt(req.params.idx, 10)
      const p = this.proxyManager.remove(idx)
      if (!p) return res.status(404).json({ error: 'Invalid index' })
      res.json({ ok: true, removed: p })
    })
    expressApp.get('/api/system', (req, res) => res.json(getSystemMetrics()))
    expressApp.get('/api/server-env', (req, res) => res.json(SERVER_ENV))

    io.on('connection', sock => {
      sock.emit('init', { bots: this.bots.map(b => this._botSummary(b)), serverEnv: SERVER_ENV })

      sock.on('subscribe', id => {
        sock.join(`bot:${id}`)
        const b = this._findBot(id)
        if (b) {
          sock.emit('logs', { id, logs: getBotLog(id).toArray() })
          sock.emit('inventory', { id, items: b.state.inventory })
          sock.emit('customCmds', { id, cmds: b.getCustomCmds() })
        }
      })
      sock.on('unsubscribe', id => sock.leave(`bot:${id}`))
      sock.on('cmd', ({ id, cmd }) => {
        if (id === '*') { for (const bot of this.bots) bot.cmd(String(cmd)) }
        else { const b = this._findBot(id); if (b) b.cmd(String(cmd)) }
      })
      sock.on('reconnect_bot', ({ id }) => { const b = this._findBot(id); if (b) b.forceReconnect() })
      sock.on('addBot', (data, cb) => {
        const { id, host, port, username, password, version, proxyIdx } = data || {}
        if (!id || !host || !port || !username) { if (cb) cb({ ok: false, msg: 'Thiếu thông tin' }); return }
        if (this._findBot(id)) { if (cb) cb({ ok: false, msg: 'ID đã tồn tại' }); return }
        const b = manager._createBotFromData({ id, host, port, username, password, version, proxyIdx })
        this.bots.push(b); b.start()
        io.emit('botAdded', this._botSummary(b))
        if (cb) cb({ ok: true })
      })
      sock.on('startBot', ({ id }, cb) => {
        const b = this._findBot(id)
        if (!b) { if (cb) cb({ ok: false, msg: 'Bot không tồn tại' }); return }
        if (b.isConnected || b.isReconnecting) { if (cb) cb({ ok: false, msg: 'Bot đang chạy rồi' }); return }
        b._disabled = false; b.state.reconnects = 0; b.start()
        io.emit('botState', { id, state: b.state.connState })
        if (cb) cb({ ok: true })
      })
      sock.on('stopBot', ({ id }, cb) => {
        const b = this._findBot(id)
        if (!b) { if (cb) cb({ ok: false, msg: 'Bot không tồn tại' }); return }
        b.shutdown()
        io.emit('botState', { id, state: b.state.connState })
        if (cb) cb({ ok: true })
      })
      sock.on('removeBot', ({ id }, cb) => {
        const b = this._findBot(id)
        if (!b) { if (cb) cb({ ok: false }); return }
        b.shutdown()
        const idx = this.bots.indexOf(b)
        if (idx !== -1) this.bots.splice(idx, 1)
        botLogs.delete(id)
        io.emit('botRemoved', { id })
        if (cb) cb({ ok: true })
      })
      sock.on('addCustomCmd', ({ id, name, cmd }, cb) => {
        const b = this._findBot(id); if (!b) { if (cb) cb({ ok: false }); return }
        b.customCommands.set(name.toLowerCase(), cmd)
        io.emit('customCmds', { id, cmds: b.getCustomCmds() })
        if (cb) cb({ ok: true })
      })
      sock.on('delCustomCmd', ({ id, name }, cb) => {
        const b = this._findBot(id); if (!b) { if (cb) cb({ ok: false }); return }
        b.customCommands.delete(name.toLowerCase())
        io.emit('customCmds', { id, cmds: b.getCustomCmds() })
        if (cb) cb({ ok: true })
      })
      sock.on('getSystemMetrics', (cb) => { if (cb) cb(getSystemMetrics()) })
    })

    expressServer.listen(this.port, () => {
      const url = `http://localhost:${this.port}`
      if (AUTO_EXE) {
        console.log(`\x1b[36m╔══════════════════════════════════════╗\x1b[0m`)
        console.log(`\x1b[36m║  ⬡  Mine Bot Manager — Antares       ║\x1b[0m`)
        console.log(`\x1b[36m╠══════════════════════════════════════╣\x1b[0m`)
        console.log(`\x1b[36m║  Web Dashboard:                      ║\x1b[0m`)
        console.log(`\x1b[36m║  \x1b[33m${url.padEnd(36)}\x1b[36m║\x1b[0m`)
        console.log(`\x1b[36m╚══════════════════════════════════════╝\x1b[0m`)
      } else {
        console.log(`\x1b[36m[Dashboard] Web UI: ${url}\x1b[0m`)
      }
    })

    this._cachedHtml = this._buildHtml()

    // BUG 4 FIX: dirty-flag diff — serialize snapshot and only emit if data has changed
    // Prevents serializing all bots every 1.5s regardless of changes (~40% CPU reduction)
    setInterval(() => {
      if (!io || io.engine.clientsCount === 0) return
      const snap = JSON.stringify(this.bots.map(b => this._botSummary(b)))
      if (snap === this._prevStatusSnap) return  // nothing changed — skip emit
      this._prevStatusSnap = snap
      io.emit('statusUpdate', { bots: JSON.parse(snap) })
    }, 1500)

    setInterval(() => {
      if (!io || io.engine.clientsCount === 0) return
      io.emit('systemMetrics', getSystemMetrics())
    }, 3000)
  }

  _findBot(id) { return this.bots.find(b => b.cfg.id.toLowerCase() === (id || '').toLowerCase()) }

  _botSummary(b) {
    const s = b.state; const pm = b.packetMgr
    return {
      id: b.cfg.id, username: b.cfg.username, host: b.cfg.host, port: b.cfg.port,
      version: b.cfg.version, state: s.connState, afk: s.afk, shard: s.shard,
      ping: s.ping, reconnects: s.reconnects, health: s.health, food: s.food,
      position: s.position,
      tshard: s.tshard, autoStats: s.autoStats, autoShard: s.autoShard,
      proxy: b.proxy ? `${b.proxy.type}://${b.proxy.host}:${b.proxy.port}` : null,
      ppsIn: pm?.ppsIn ?? 0, ppsOut: pm?.ppsOut ?? 0, lastPacket: pm?.lastPacketAt ?? null,
      loginTime: s.loginTime,
      menuRetries: b._menuRetryCount ?? 0,
      menuSuccess: b._menuSuccess ?? false,
      cfg: { autoMenu: b.cfg.autoMenu, menuCommand: b.cfg.menuCommand, respawn: b.cfg.respawn, ownerUsername: b.cfg.ownerUsername, useProxy: b.cfg.useProxy },
    }
  }

  _buildHtml() {
    return `<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>⬡ Mine Bot — Antares Generator</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#070910;--bg2:#0b0e18;--bg3:#10141f;--bg4:#141925;--bg5:#18202e;
  --border:#1c2438;--border2:#22304a;--border3:#2a3d60;
  --text:#c8d8ee;--text2:#6a84a8;--text3:#3a5070;--text4:#243048;
  --cyan:#00e0cc;--cyan2:#00b8a8;--cyan3:rgba(0,224,204,.13);
  --green:#32e878;--green2:#18b858;--green3:rgba(50,232,120,.11);
  --red:#ff4c64;--red2:#c82840;--red3:rgba(255,76,100,.11);
  --yellow:#ffc820;--yellow2:#c89808;--yellow3:rgba(255,200,32,.09);
  --blue:#46a0ff;--blue2:#2070d8;--blue3:rgba(70,160,255,.11);
  --purple:#a860ff;--purple2:#7838c8;--purple3:rgba(168,96,255,.11);
  --orange:#ff8030;
  --grad1:linear-gradient(135deg,#00e0cc,#46a0ff);
  --grad2:linear-gradient(135deg,#a860ff,#46a0ff);
  --grad3:linear-gradient(135deg,#00e0cc,#32e878);
  --r:7px;--r2:11px;--r3:15px;
  --font:'Inter',sans-serif;--mono:'JetBrains Mono',monospace;
  --sh:0 4px 20px rgba(0,0,0,.65);--sh2:0 8px 36px rgba(0,0,0,.85);
  --glow-c:0 0 18px rgba(0,224,204,.18),0 0 36px rgba(0,224,204,.07);
  --glow-g:0 0 18px rgba(50,232,120,.13);
  --glow-r:0 0 18px rgba(255,76,100,.18);
}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:13px;line-height:1.5}
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border3);border-radius:3px}

.app{display:flex;height:100vh;flex-direction:column}

.topbar{
  display:flex;align-items:center;gap:10px;padding:0 14px;
  background:var(--bg2);height:48px;border-bottom:1px solid var(--border);
  flex-shrink:0;position:relative;z-index:10;
}
.topbar::after{
  content:'';position:absolute;bottom:-1px;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(0,224,204,.25),transparent);
}
.tl{display:flex;align-items:center;gap:8px;font-size:14px;font-weight:700}
.tl-hex{font-size:20px;background:var(--grad1);-webkit-background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 5px rgba(0,224,204,.4))}
.tl-txt{background:var(--grad1);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.tl-sep{color:var(--border3);margin:0 3px}
.pill{font-size:10px;padding:2px 9px;border-radius:20px;border:1px solid var(--border2);color:var(--text2);white-space:nowrap;font-family:var(--mono)}
.pill.on{border-color:var(--green2);color:var(--green);background:var(--green3)}
.pill.exe{border-color:var(--purple2);color:var(--purple);background:var(--purple3)}
#hdr-time{font-size:10px;color:var(--text3);margin-left:auto;font-family:var(--mono)}

.body{display:flex;flex:1;overflow:hidden}

.sidebar{
  width:220px;min-width:150px;flex-shrink:0;background:var(--bg2);
  border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden;
}
.sb-hdr{
  padding:10px 13px 8px;font-size:9px;font-weight:700;letter-spacing:1.8px;
  color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;
}
.sb-list{flex:1;overflow-y:auto;padding:6px}
.bi{
  border:1px solid transparent;border-radius:var(--r);padding:8px 10px;margin-bottom:4px;
  cursor:pointer;transition:all .12s;position:relative;overflow:hidden;
}
.bi::before{content:'';position:absolute;left:0;top:0;bottom:0;width:2px;background:transparent;border-radius:0 2px 2px 0;transition:background .12s}
.bi:hover{background:var(--bg3);border-color:var(--border2)}
.bi.active{background:var(--bg3);border-color:var(--cyan2)}
.bi.active::before{background:var(--cyan)}
.bi-r1{display:flex;align-items:center;gap:6px;margin-bottom:2px}
.dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;transition:all .25s}
.dot.ONLINE{background:var(--green);box-shadow:0 0 6px var(--green2)}
.dot.OFFLINE,.dot.DISCONNECTED{background:var(--red2)}
.dot.STOPPED{background:var(--text3)}
.dot.RECONNECTING,.dot.CONNECTING,.dot.SPAWNING,.dot.AUTHENTICATING{background:var(--yellow);animation:dp 1.2s infinite}
.dot.STOPPING{background:var(--text3)}
@keyframes dp{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.6)}}
.bn{font-weight:600;font-size:12px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text)}
.bpg{font-size:9px;color:var(--text3);font-family:var(--mono)}
.bsub{font-size:9px;color:var(--text2);display:flex;gap:5px;flex-wrap:wrap;margin-top:1px}
.bsub span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:95px}
.bpkt{font-size:9px;color:var(--cyan);font-family:var(--mono);opacity:.6;margin-top:2px}
.sb-add{
  margin:6px;border:1px dashed var(--border3);border-radius:var(--r);padding:7px;
  text-align:center;cursor:pointer;font-size:11px;color:var(--text2);transition:all .18s;
  background:transparent;font-family:var(--font);display:flex;align-items:center;justify-content:center;gap:5px;
}
.sb-add:hover{border-color:var(--cyan2);color:var(--cyan);background:var(--cyan3)}

.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.no-sel{flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:12px;color:var(--text3)}
.no-sel-icon{font-size:48px;opacity:.15;filter:grayscale(1)}
.no-sel-txt{font-size:12px}.no-sel-sub{font-size:10px;color:var(--text4)}

.tabs{
  display:flex;align-items:flex-end;gap:0;padding:0 12px;
  background:var(--bg2);border-bottom:1px solid var(--border);
  flex-shrink:0;overflow-x:auto;scrollbar-width:none;min-height:42px;
}
.tabs::-webkit-scrollbar{display:none}
.tab{
  padding:9px 14px;font-size:11px;cursor:pointer;color:var(--text3);
  border:none;border-bottom:2px solid transparent;transition:all .12s;
  white-space:nowrap;background:transparent;font-family:var(--font);font-weight:500;
}
.tab:hover{color:var(--text)}
.tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}

.tc{flex:1;overflow:hidden;position:relative}
.tp{display:none;height:100%;overflow-y:auto;padding:14px;animation:fi .12s}
.tp.active{display:block}
@keyframes fi{from{opacity:0;transform:translateY(2px)}to{opacity:1;transform:translateY(0)}}

.sec{font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);margin-bottom:9px;display:flex;align-items:center;gap:7px}
.sec::after{content:'';flex:1;height:1px;background:var(--border)}

.sg{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:7px;margin-bottom:12px}
.sb{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:11px 13px;transition:all .18s;cursor:default;position:relative;overflow:hidden}
.sb:hover{border-color:var(--border3);transform:translateY(-1px)}
.sb::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;background:transparent;transition:background .18s}
.sb:hover::before{background:linear-gradient(90deg,transparent,var(--cyan2),transparent)}
.sl{font-size:9px;color:var(--text3);letter-spacing:.9px;margin-bottom:4px;text-transform:uppercase;font-weight:600}
.sv{font-size:17px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono)}
.sv.c-green{color:var(--green)}.sv.c-red{color:var(--red)}.sv.c-yellow{color:var(--yellow)}
.sv.c-blue{color:var(--blue)}.sv.c-purple{color:var(--purple)}.sv.c-cyan{color:var(--cyan)}
.sv.c-online{color:var(--green)}.sv.c-offline,.sv.c-disconnected{color:var(--red)}
.sv.c-reconnecting,.sv.c-connecting,.sv.c-spawning,.sv.c-authenticating{color:var(--yellow)}
.sv.c-stopped{color:var(--text3)}

.pw{display:flex;gap:7px;margin-bottom:12px}
.pb{flex:1;background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:9px 13px;display:flex;align-items:center;gap:9px;transition:border-color .18s}
.pb.in{border-left:3px solid var(--green2)}.pb.out{border-left:3px solid var(--blue2)}
.pi-ico{font-size:16px;flex-shrink:0}
.pi-info{flex:1}
.pi-lbl{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.9px}
.pi-val{font-size:18px;font-weight:700;font-family:var(--mono);color:var(--text)}
.pi-unit{font-size:9px;color:var(--text2);margin-left:2px}

.pd{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:10px 13px;margin-bottom:12px}
.pd-lbl{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.9px;margin-bottom:6px;font-weight:600}
.pd-coords{display:flex;gap:14px;flex-wrap:wrap}
.co{display:flex;gap:4px;align-items:center}
.co-k{color:var(--text3);font-size:11px;font-family:var(--mono)}
.co-v{color:var(--cyan);font-weight:700;font-family:var(--mono)}

.menu-status{
  background:var(--bg3);border:1px solid var(--border2);border-radius:var(--r);
  padding:9px 13px;margin-bottom:12px;display:flex;align-items:center;gap:10px;
  font-size:11px;
}
.menu-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.menu-dot.success{background:var(--green);box-shadow:var(--glow-g)}
.menu-dot.retrying{background:var(--yellow);animation:dp 1s infinite}
.menu-dot.idle{background:var(--text3)}
.menu-info{flex:1}
.menu-label{color:var(--text2);font-size:10px}
.menu-val{color:var(--text);font-weight:600;font-size:11px;font-family:var(--mono)}

.abar{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:12px}
.btn{
  background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);
  color:var(--text2);padding:5px 12px;cursor:pointer;font-size:11px;
  font-family:var(--font);transition:all .12s;white-space:nowrap;
  line-height:1.4;font-weight:500;display:inline-flex;align-items:center;gap:4px;
}
.btn:hover{background:var(--bg4);border-color:var(--border2);color:var(--text)}
.btn.pr{border-color:var(--cyan2);color:var(--cyan)}.btn.pr:hover{background:var(--cyan3);box-shadow:var(--glow-c)}
.btn.sc{border-color:var(--green2);color:var(--green)}.btn.sc:hover{background:var(--green3)}
.btn.dr{border-color:var(--red2);color:var(--red)}.btn.dr:hover{background:var(--red3)}
.btn.wn{border-color:var(--yellow2);color:var(--yellow)}.btn.wn:hover{background:var(--yellow3)}
.btn.pu{border-color:var(--purple2);color:var(--purple)}.btn.pu:hover{background:var(--purple3)}
.btn.sm{padding:3px 8px;font-size:10px}
.btn:active{transform:scale(.96)}
.btn.active{background:var(--cyan3);border-color:var(--cyan);color:var(--cyan);box-shadow:var(--glow-c)}

.log-wrap{display:flex;flex-direction:column;gap:7px}
.log-tb{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.log-tb .sp{flex:1}
.lbox{
  background:var(--bg2);border:1px solid var(--border);border-radius:var(--r);
  height:calc(100vh - 280px);min-height:200px;overflow-y:auto;
  padding:8px 10px;font-size:11px;line-height:1.75;font-family:var(--mono);
  contain:size layout style;
}
.ll{word-break:break-all;padding:1px 0;border-bottom:1px solid rgba(255,255,255,.015);display:flex;gap:6px;align-items:flex-start}
.ll-time{color:var(--text4);font-size:9px;flex-shrink:0;padding-top:2px;user-select:none;width:60px}
.ll-badge{flex-shrink:0;font-size:9px;padding:1px 5px;border-radius:3px;margin-top:2px;font-weight:700;letter-spacing:.5px}
.ll-msg{flex:1;word-break:break-all}
.ll.ok .ll-badge{background:rgba(50,232,120,.15);color:var(--green)}
.ll.ok .ll-msg{color:var(--green)}
.ll.warn .ll-badge{background:rgba(255,200,32,.12);color:var(--yellow)}
.ll.warn .ll-msg{color:var(--yellow)}
.ll.err .ll-badge{background:rgba(255,76,100,.15);color:var(--red)}
.ll.err .ll-msg{color:var(--red)}
.ll.chat .ll-badge{background:rgba(168,96,255,.13);color:var(--purple)}
.ll.chat .ll-msg{color:#c090ff}
.ll.shard .ll-badge{background:rgba(0,224,204,.12);color:var(--cyan)}
.ll.shard .ll-msg{color:var(--cyan)}
.ll.afk .ll-badge{background:rgba(255,200,32,.1);color:var(--yellow)}
.ll.afk .ll-msg{color:var(--yellow)}
.ll.sys .ll-badge{background:rgba(100,100,170,.1);color:#7878b0}
.ll.sys .ll-msg{color:var(--text2)}
.ll.health .ll-badge{background:rgba(255,76,100,.12);color:#ff8090}
.ll.health .ll-msg{color:#ff8090}
.ll.proxy .ll-badge{background:rgba(168,96,255,.1);color:var(--purple2)}
.ll.proxy .ll-msg{color:var(--purple)}

.ll-new{animation:flashIn .3s ease}
@keyframes flashIn{from{background:rgba(0,224,204,.06)}to{background:transparent}}

.cb{display:flex;gap:5px;margin-top:6px}
.ci{
  flex:1;background:var(--bg2);border:1px solid var(--border);border-radius:var(--r);
  color:var(--text);padding:7px 11px;font-family:var(--mono);font-size:12px;outline:none;transition:border-color .12s;min-width:0;
}
.ci:focus{border-color:var(--cyan2);box-shadow:0 0 0 2px rgba(0,224,204,.07)}

.ig{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:5px;margin-bottom:10px}
.ii{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:8px 10px;display:flex;align-items:center;gap:8px;transition:all .12s}
.ii:hover{border-color:var(--border3)}
.isl{font-size:9px;color:var(--text3);width:22px;flex-shrink:0;text-align:right;font-family:var(--mono)}
.iico{font-size:15px;flex-shrink:0}
.idet{flex:1;min-width:0}
.inm{font-size:11px;font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ict{font-size:10px;color:var(--cyan);font-family:var(--mono)}
.ity{font-size:9px;color:var(--text3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.inv-empty{color:var(--text3);font-size:12px;text-align:center;padding:40px;grid-column:1/-1}

.sys-sec{margin-bottom:16px}
.sys-t{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--text3);margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border)}
.sys-g{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:7px}
.sys-b{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:11px}
.sys-l{font-size:9px;color:var(--text3);margin-bottom:3px;text-transform:uppercase;letter-spacing:.9px;font-weight:600}
.sys-v{font-size:15px;font-weight:700;color:var(--cyan);font-family:var(--mono)}
.sys-s{font-size:10px;color:var(--text3);margin-top:2px}
.bar-w{margin-top:5px}
.bar{height:4px;background:var(--bg);border-radius:3px;overflow:hidden}
.bf{height:100%;border-radius:3px;transition:width .5s ease}
.bf.safe{background:linear-gradient(90deg,var(--green2),var(--cyan2))}
.bf.warn{background:linear-gradient(90deg,var(--yellow2),var(--orange))}
.bf.danger{background:linear-gradient(90deg,var(--red2),var(--orange))}
.bar-p{font-size:9px;color:var(--text3);margin-top:2px;font-family:var(--mono)}

.mg{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:9px;margin-bottom:12px}
.mc{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r2);padding:13px;transition:all .18s;position:relative;overflow:hidden}
.mc::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--border)}
.mc.co{cursor:pointer}
.mc.card-online::before{background:var(--grad3)}
.mc.card-reconnecting::before,.mc.card-connecting::before{background:linear-gradient(90deg,var(--yellow2),var(--orange))}
.mc.card-offline::before,.mc.card-disconnected::before{background:var(--red2)}
.mc.card-stopped::before{background:var(--border3)}
.mc:hover{border-color:var(--border3)}
.mch{display:flex;align-items:center;gap:7px;margin-bottom:8px}
.mcd{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.mcd.ONLINE{background:var(--green);box-shadow:0 0 7px var(--green2)}
.mcd.DISCONNECTED,.mcd.STOPPED{background:var(--red2)}
.mcd.RECONNECTING,.mcd.CONNECTING,.mcd.SPAWNING{background:var(--yellow);animation:dp 1.2s infinite}
.mcid{font-weight:700;font-size:13px;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mcs{font-size:9px;padding:2px 7px;border-radius:20px;font-family:var(--mono);font-weight:600}
.mcs.ONLINE{background:var(--green3);color:var(--green);border:1px solid var(--green2)}
.mcs.DISCONNECTED,.mcs.STOPPED{background:var(--red3);color:var(--red);border:1px solid var(--red2)}
.mcs.RECONNECTING,.mcs.CONNECTING,.mcs.SPAWNING{background:var(--yellow3);color:var(--yellow);border:1px solid var(--yellow2)}
.mci{display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-bottom:8px;font-size:10px}
.mcir{display:flex;flex-direction:column}
.bcl{color:var(--text3);font-size:9px;margin-bottom:1px}
.bcv{color:var(--text2);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mcp{background:var(--bg2);border:1px solid var(--border);border-radius:5px;padding:5px 9px;display:flex;justify-content:space-between;margin-bottom:8px;font-family:var(--mono);font-size:10px}
.pin{color:var(--green)}.pout{color:var(--blue)}
.mca{display:flex;gap:4px;flex-wrap:wrap}

.cs{background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:13px;margin-bottom:10px}
.ct{font-size:10px;font-weight:700;color:var(--text2);margin-bottom:10px;text-transform:uppercase;letter-spacing:.9px}
.cr{display:flex;align-items:center;gap:9px;margin-bottom:9px;flex-wrap:wrap}
.cl{font-size:11px;color:var(--text2);width:125px;flex-shrink:0}
.cin{flex:1;min-width:110px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--r);color:var(--text);padding:5px 9px;font-family:var(--mono);font-size:11px;outline:none}
.cin:focus{border-color:var(--cyan2);box-shadow:0 0 0 2px rgba(0,224,204,.07)}
.ctog{position:relative;width:33px;height:17px;cursor:pointer;flex-shrink:0}
.ctog input{opacity:0;width:0;height:0;position:absolute}
.ts{position:absolute;inset:0;background:var(--bg);border:1px solid var(--border2);border-radius:9px;transition:.18s}
.ts:before{content:'';position:absolute;height:11px;width:11px;left:2px;bottom:2px;background:var(--text3);border-radius:50%;transition:.18s}
input:checked + .ts{background:var(--cyan2);border-color:var(--cyan2)}
input:checked + .ts:before{transform:translateX(16px);background:#fff}

.cl-list{display:flex;flex-direction:column;gap:3px;margin-bottom:9px}
.cli{display:flex;align-items:center;gap:7px;background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:6px 10px}
.cln{font-weight:700;color:var(--cyan);font-size:11px;min-width:75px;font-family:var(--mono)}
.cla{color:var(--text4);font-size:10px}
.clv{flex:1;color:var(--text2);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono)}
.cld{cursor:pointer;color:var(--text3);font-size:12px;flex-shrink:0;padding:2px 3px;border-radius:3px;transition:color .12s}
.cld:hover{color:var(--red)}

.acr{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.pl{display:flex;flex-direction:column;gap:3px;margin-bottom:9px}
.pi{display:flex;align-items:center;gap:9px;background:var(--bg3);border:1px solid var(--border);border-radius:var(--r);padding:7px 10px}
.pidx{font-size:9px;color:var(--text3);width:18px;text-align:center;flex-shrink:0;font-family:var(--mono)}
.ptyp{font-size:9px;padding:2px 6px;border-radius:3px;background:var(--bg);border:1px solid var(--border2);color:var(--purple);flex-shrink:0;font-family:var(--mono)}
.pho{flex:1;font-size:11px;color:var(--cyan);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono)}
.pdel{cursor:pointer;color:var(--text3);padding:2px 4px;border-radius:3px;font-size:12px;flex-shrink:0}
.pdel:hover{color:var(--red)}

.overlay{position:fixed;inset:0;background:rgba(0,0,0,.78);z-index:200;display:none;align-items:center;justify-content:center;padding:20px;backdrop-filter:blur(8px)}
.overlay.open{display:flex;animation:fi .12s}
.modal{background:var(--bg2);border:1px solid var(--border2);border-radius:var(--r2);padding:22px;width:100%;max-width:480px;max-height:90vh;overflow-y:auto;box-shadow:var(--sh2);position:relative;overflow:hidden}
.modal::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--grad1)}
.mt{font-size:14px;font-weight:700;color:var(--text);margin-bottom:16px;display:flex;align-items:center;justify-content:space-between}
.mc2{cursor:pointer;color:var(--text2);font-size:15px;padding:3px 7px;border-radius:var(--r);transition:all .12s}
.mc2:hover{color:var(--text);background:var(--bg3)}
.fr{margin-bottom:10px}
.fl{display:block;font-size:9px;text-transform:uppercase;letter-spacing:.9px;color:var(--text3);margin-bottom:4px;font-weight:600}
.fi{width:100%;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);color:var(--text);padding:8px 11px;font-family:var(--mono);font-size:12px;outline:none}
.fi:focus{border-color:var(--cyan2);box-shadow:0 0 0 2px rgba(0,224,204,.07)}
.fg2{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.mf{display:flex;gap:7px;margin-top:16px;justify-content:flex-end}

.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%) translateY(6px);padding:9px 18px;border-radius:50px;font-size:11px;font-family:var(--font);z-index:9998;opacity:0;transition:all .22s;pointer-events:none;border:1px solid;backdrop-filter:blur(8px);white-space:nowrap}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.toast.green{background:rgba(24,80,48,.9);border-color:var(--green2);color:var(--green)}
.toast.red{background:rgba(100,16,24,.9);border-color:var(--red2);color:var(--red)}
.toast.yellow{background:rgba(80,60,0,.9);border-color:var(--yellow2);color:var(--yellow)}

.live-badge{
  display:inline-flex;align-items:center;gap:4px;font-size:9px;
  padding:2px 8px;border-radius:20px;background:var(--red3);
  border:1px solid var(--red2);color:var(--red);font-weight:700;
}
.live-dot{width:5px;height:5px;border-radius:50%;background:var(--red);animation:dp .9s infinite}

@media(max-width:700px){.sidebar{width:170px}.sg{grid-template-columns:repeat(2,1fr)}.sys-g{grid-template-columns:repeat(2,1fr)}}
@media(max-width:480px){.sidebar{display:none}.tp{padding:9px}.sg{grid-template-columns:1fr 1fr}.pw{flex-direction:column}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="tl">
      <span class="tl-hex">⬡</span>
      <span class="tl-txt">Antares Manager</span>
    </div>
    <span class="tl-sep">│</span>
    <span id="hdr-count" class="pill">0 bots</span>
    <span id="hdr-online" class="pill on" style="display:none">0 online</span>
    ${AUTO_EXE ? '<span class="pill exe">⬡ Auto-EXE</span>' : ''}
    <div class="live-badge" style="margin-left:4px"><div class="live-dot"></div>LIVE</div>
    <span id="hdr-time"></span>
  </div>
  <div class="body">
    <div class="sidebar">
      <div class="sb-hdr">
        <span>Bots</span>
        <span id="sb-cnt" style="color:var(--text4);font-size:9px;font-family:var(--mono)"></span>
      </div>
      <div class="sb-list" id="sb-list"></div>
      <div class="sb-add" id="btn-add-open">＋ Thêm bot mới</div>
    </div>
    <div class="main" id="main">
      <div class="no-sel" id="no-sel">
        <div class="no-sel-icon">⬡</div>
        <div class="no-sel-txt">Chọn bot để xem chi tiết</div>
        <div class="no-sel-sub">hoặc vào Bot Manager để quản lý tất cả</div>
      </div>
      <div id="bv" style="display:none;flex:1;flex-direction:column;overflow:hidden">
        <div class="tabs" id="tabs">
          <button class="tab active" data-tab="overview">📊 Overview</button>
          <button class="tab" data-tab="manager">🤖 Manager</button>
          <button class="tab" data-tab="logs">📋 Logs</button>
          <button class="tab" data-tab="inventory">🎒 Inventory</button>
          <button class="tab" data-tab="system">💻 System</button>
          <button class="tab" data-tab="config">⚙️ Config</button>
          <button class="tab" data-tab="proxy">🔀 Proxy</button>
        </div>
        <div class="tc">
          <div class="tp active" id="tab-overview">
            <div class="abar" id="abar"></div>
            <div id="menu-status-box" style="display:none" class="menu-status">
              <div class="menu-dot idle" id="menu-dot"></div>
              <div class="menu-info">
                <div class="menu-label">Auto Menu</div>
                <div class="menu-val" id="menu-val">—</div>
              </div>
            </div>
            <div class="pw" id="pw">
              <div class="pb in"><div class="pi-ico">📥</div><div class="pi-info"><div class="pi-lbl">Packets IN</div><div><span class="pi-val" id="pki">—</span><span class="pi-unit">/s</span></div></div></div>
              <div class="pb out"><div class="pi-ico">📤</div><div class="pi-info"><div class="pi-lbl">Packets OUT</div><div><span class="pi-val" id="pko">—</span><span class="pi-unit">/s</span></div></div></div>
            </div>
            <div class="sg" id="sg"></div>
            <div class="pd" id="pd">
              <div class="pd-lbl">Vị trí</div>
              <div class="pd-coords" id="pdc"><span style="color:var(--text4)">—</span></div>
            </div>
          </div>
          <div class="tp" id="tab-manager">
            <div class="sec">Tất cả Bot</div>
            <div class="mg" id="mg"></div>
            <div style="display:flex;gap:7px;margin-top:3px;flex-wrap:wrap">
              <button class="btn sc" id="btn-sa">▶ Start All</button>
              <button class="btn dr" id="btn-xa">■ Stop All</button>
              <button class="btn pr" id="btn-add-open2">＋ Thêm Bot</button>
            </div>
          </div>
          <div class="tp" id="tab-logs">
            <div class="log-wrap">
              <div class="log-tb">
                <span style="font-size:11px;color:var(--text2)">Live Log</span>
                <span id="log-cnt" style="font-size:10px;color:var(--text3);font-family:var(--mono)"></span>
                <span class="sp"></span>
                <button class="btn sm" id="btn-lc">🗑 Xóa</button>
                <label style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--text2);cursor:pointer">
                  <input type="checkbox" id="log-as" checked style="cursor:pointer"> Auto-scroll
                </label>
              </div>
              <div class="lbox" id="lbox"></div>
              <div class="cb">
                <input class="ci" id="ci" placeholder="Nhập lệnh (afk, stats, /say hi, ...)">
                <button class="btn pr" id="btn-send">Gửi ▶</button>
              </div>
            </div>
          </div>
          <div class="tp" id="tab-inventory">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
              <span style="font-size:11px;color:var(--text2)" id="inv-sum">Đang tải...</span>
              <button class="btn sm" id="btn-inv-ref">↻ Refresh</button>
            </div>
            <div class="ig" id="ig"></div>
          </div>
          <div class="tp" id="tab-system">
            <div class="sys-sec"><div class="sys-t">Memory</div><div class="sys-g" id="sys-mem"></div></div>
            <div class="sys-sec"><div class="sys-t">CPU & Load</div><div class="sys-g" id="sys-cpu"></div></div>
            <div class="sys-sec"><div class="sys-t">Process (Node.js)</div><div class="sys-g" id="sys-proc"></div></div>
            <div class="sys-sec"><div class="sys-t">Hệ thống</div><div class="sys-g" id="sys-info"></div></div>
          </div>
          <div class="tp" id="tab-config">
            <div class="cs">
              <div class="ct">Cài đặt Bot</div>
              <div class="cr"><span class="cl">Owner Username</span><input class="cin" id="cfg-ow" placeholder="username"></div>
              <div class="cr"><span class="cl">Menu Command</span><input class="cin" id="cfg-mc" placeholder="/menu"></div>
              <div class="cr"><span class="cl">Auto Menu</span><label class="ctog"><input type="checkbox" id="cfg-am"><span class="ts"></span></label></div>
              <div class="cr"><span class="cl">Auto Respawn</span><label class="ctog"><input type="checkbox" id="cfg-rs"><span class="ts"></span></label></div>
              <div class="cr"><span class="cl">Dùng Proxy</span><label class="ctog"><input type="checkbox" id="cfg-up"><span class="ts"></span></label></div>
              <div style="margin-top:10px"><button class="btn pr" id="btn-sc">✓ Lưu</button></div>
            </div>
            <div class="cs">
              <div class="ct">Custom Commands</div>
              <div class="cl-list" id="ccl"></div>
              <div class="acr">
                <input class="cin" id="nc-n" placeholder="tên lệnh" style="flex:0 0 110px">
                <input class="cin" id="nc-v" placeholder="/lệnh MC" style="flex:1">
                <button class="btn sc sm" id="btn-ac">＋ Thêm</button>
              </div>
            </div>
            <div class="cs" style="border-color:var(--red2)">
              <div class="ct" style="color:var(--red)">Danger Zone</div>
              <div style="display:flex;gap:7px;flex-wrap:wrap">
                <button class="btn dr" id="btn-stop">■ Tắt Bot</button>
                <button class="btn wn" id="btn-rm">⊗ Xóa Bot</button>
              </div>
            </div>
          </div>
          <div class="tp" id="tab-proxy">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
              <span style="font-size:11px;color:var(--text2)" id="px-sum">Đang tải...</span>
              <button class="btn pr sm" id="btn-pxo">＋ Thêm Proxy</button>
            </div>
            <div class="pl" id="px-list"></div>
            <div id="pxr" style="display:none;gap:5px;margin-top:7px" class="acr">
              <input class="cin" id="px-val" placeholder="socks5://user:pass@host:port" style="flex:1">
              <button class="btn sc sm" id="btn-pxa">＋</button>
              <button class="btn sm" id="btn-pxc">✕</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<div class="overlay" id="modal-add">
  <div class="modal">
    <div class="mt">
      <span>⬡ Thêm Bot Mới</span>
      <span class="mc2" id="btn-mc">✕</span>
    </div>
    <div class="fg2">
      <div class="fr"><label class="fl">ID *</label><input class="fi" id="nb-id" placeholder="bot-2"></div>
      <div class="fr"><label class="fl">Username *</label><input class="fi" id="nb-u" placeholder="Steve"></div>
    </div>
    <div class="fg2">
      <div class="fr"><label class="fl">Host *</label><input class="fi" id="nb-h" placeholder="play.example.com"></div>
      <div class="fr"><label class="fl">Port *</label><input class="fi" id="nb-p" placeholder="25565" type="number"></div>
    </div>
    <div class="fg2">
      <div class="fr"><label class="fl">Password</label><input class="fi" id="nb-pw" type="password"></div>
      <div class="fr"><label class="fl">Version</label><input class="fi" id="nb-v" placeholder="1.20.1"></div>
    </div>
    <div class="fr"><label class="fl">Proxy Index</label><input class="fi" id="nb-px" placeholder="0, 1, 2... (trống = auto)"></div>
    <div id="merr" style="color:var(--red);font-size:11px;margin-top:7px;display:none"></div>
    <div class="mf">
      <button class="btn" id="btn-mcan">Hủy</button>
      <button class="btn pr" id="btn-msub">✓ Tạo Bot</button>
    </div>
  </div>
</div>

<script src="/socket.io/socket.io.js"></script>
<script>
let bots=[], activeId=null, activeTab='overview'
let logQ=[], logPend=false, logLineCount=0
let sysM=null, srvEnv=null, pxData=[], ccData={}, invData={}
const BADGE_LABELS={ok:'OK',warn:'WRN',err:'ERR',shard:'◈',chat:'MSG',sys:'SYS',afk:'AFK',pkt:'PKT',proxy:'PRX',health:'♥'}
const $=id=>document.getElementById(id)
const esc=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
const getBot=id=>bots.find(b=>b.id===id)
const fmtMem=b=>{if(b>1073741824)return(b/1073741824).toFixed(1)+'GB';if(b>1048576)return(b/1048576).toFixed(0)+'MB';return(b/1024).toFixed(0)+'KB'}
const fmtUp=s=>{const d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);if(d>0)return d+'d '+h+'h';if(h>0)return h+'h '+m+'m';return m+'m '+(Math.floor(s)%60)+'s'}
const fmtEl=ms=>{if(!ms)return'—';const s=Math.floor((Date.now()-ms)/1000);const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return(h>0?h+'h ':'')+m+'m '+sc+'s'}
const stCls=s=>({'ONLINE':'online','RECONNECTING':'reconnecting','CONNECTING':'connecting','SPAWNING':'spawning','AUTHENTICATING':'connecting','STOPPING':'stopped','DISCONNECTED':'disconnected'}[s]||'offline')
const dotCls=s=>({'ONLINE':'ONLINE','RECONNECTING':'RECONNECTING','CONNECTING':'CONNECTING','SPAWNING':'SPAWNING','AUTHENTICATING':'CONNECTING','STOPPING':'STOPPED','DISCONNECTED':'DISCONNECTED'}[s]||'DISCONNECTED')

const socket=io({transports:['websocket','polling']})

socket.on('connect',()=>{socket.emit('getSystemMetrics',m=>{if(m){sysM=m}})})
socket.on('disconnect',()=>{})

socket.on('init',d=>{
  bots=d.bots||[];srvEnv=d.serverEnv||null
  renderSB();renderTop()
  if(activeId){const b=getBot(activeId);if(b)renderBV(b)}
  renderMgr()
})
socket.on('statusUpdate',d=>{
  const upd=d.bots||[]
  for(const nb of upd){const i=bots.findIndex(b=>b.id===nb.id);if(i>=0)bots[i]=nb;else bots.push(nb)}
  renderSB();renderTop()
  if(activeId){const b=getBot(activeId);if(b){renderAbar(b);updateStats(b);updatePos(b);updatePkt(b);updateMenuStatus(b)}}
  if(activeTab==='manager')renderMgr()
})
// BUG 3 FIX: log events now only come via bot-room subscription (io.to('bot:id'))
// log_broadcast handler removed — no longer needed
socket.on('log',e=>{if(e.id!==activeId)return;logQ.push(e);if(!logPend){logPend=true;requestAnimationFrame(flushLogs)}})
socket.on('logs',d=>{
  if(d.id!==activeId)return
  // BUG 5 FIX: replace logQ with full log history, flushLogs handles DOM append+trim
  logQ=[...(d.logs||[])]
  logLineCount=0
  // Clear existing DOM before loading history
  const lb=$('lbox');if(lb)lb.innerHTML=''
  if(!logPend){logPend=true;requestAnimationFrame(flushLogs)}
})
socket.on('inventory',d=>{invData[d.id]=d.items||[];if(d.id===activeId&&activeTab==='inventory')renderInv(d.items||[])})
socket.on('customCmds',d=>{ccData[d.id]=d.cmds||[];if(d.id===activeId&&activeTab==='config')renderCC(d.cmds||[])})
socket.on('systemMetrics',m=>{sysM=m;if(activeTab==='system')renderSys()})
socket.on('shard',d=>{const b=getBot(d.id);if(b)b.shard=d.shard;if(d.id===activeId){updateStats(getBot(d.id));renderAbar(getBot(d.id))};if(activeTab==='manager')renderMgr()})
socket.on('ping',d=>{const b=getBot(d.id);if(b){b.ping=d.ping};renderSB()})
socket.on('health',d=>{const b=getBot(d.id);if(b){b.health=d.health;b.food=d.food}})
socket.on('afk',d=>{const b=getBot(d.id);if(b)b.afk=d.mode;renderSB();if(activeId===d.id&&b)renderAbar(b)})
socket.on('botState',d=>{const b=getBot(d.id);if(b)b.state=d.state;renderSB();renderTop();if(activeTab==='manager')renderMgr();if(activeId===d.id&&b)renderAbar(b)})
socket.on('botAdded',b=>{if(!getBot(b.id))bots.push(b);renderSB();renderTop();renderMgr()})
socket.on('botRemoved',({id})=>{bots=bots.filter(b=>b.id!==id);renderSB();renderTop();renderMgr();if(activeId===id){activeId=null;showNS()}})

function renderSB(){
  const list=$('sb-list');if(!list)return
  list.innerHTML=bots.map(b=>{
    const dc=dotCls(b.state)
    return'<div class="bi'+(b.id===activeId?' active':'')+'" data-id="'+esc(b.id)+'">' +
      '<div class="bi-r1"><div class="dot '+dc+'"></div><div class="bn">'+esc(b.id)+'</div><div class="bpg">'+(b.ping>=0?b.ping+'ms':'')+'</div></div>' +
      '<div class="bsub"><span>'+esc(b.host+':'+b.port)+'</span>'+(b.afk?'<span>⌚'+esc(b.afk.toUpperCase())+'</span>':'')+(b.shard>0?'<span>◈'+b.shard.toLocaleString()+'</span>':'')+'</div>' +
      (b.state==='ONLINE'?'<div class="bpkt">↓'+b.ppsIn+' ↑'+b.ppsOut+'</div>':'') +
    '</div>'
  }).join('')
  list.querySelectorAll('.bi[data-id]').forEach(el=>el.addEventListener('click',()=>selBot(el.dataset.id)))
  const sc=$('sb-cnt');if(sc)sc.textContent=bots.length
}
function renderTop(){
  const hc=$('hdr-count');if(hc)hc.textContent=bots.length+' bots'
  const on=bots.filter(b=>b.state==='ONLINE').length
  const ho=$('hdr-online');if(ho){if(on>0){ho.style.display='';ho.textContent=on+' online'}else ho.style.display='none'}
}
function renderMgr(){
  const g=$('mg');if(!g)return
  if(!bots.length){g.innerHTML='<div style="color:var(--text3);font-size:12px;padding:20px;grid-column:1/-1;text-align:center">Chưa có bot nào.</div>';return}
  g.innerHTML=bots.map(b=>{
    const sc=stCls(b.state),dc=dotCls(b.state)
    const isOn=b.state==='ONLINE',isCon=['RECONNECTING','CONNECTING','SPAWNING','AUTHENTICATING'].includes(b.state)
    return'<div class="mc card-'+sc+'">' +
      '<div class="mch"><div class="mcd '+dc+'"></div><div class="mcid">'+esc(b.id)+'</div><span class="mcs '+b.state+'">'+b.state+'</span></div>' +
      '<div class="mci">' +
        '<div class="mcir"><span class="bcl">Server</span><span class="bcv">'+esc(b.host+':'+b.port)+'</span></div>' +
        '<div class="mcir"><span class="bcl">User</span><span class="bcv">'+esc(b.username||'')+'</span></div>' +
        '<div class="mcir"><span class="bcl">Ping</span><span class="bcv">'+(b.ping>=0?b.ping+'ms':'—')+'</span></div>' +
        '<div class="mcir"><span class="bcl">Shard</span><span class="bcv">'+(b.shard>0?b.shard.toLocaleString():'—')+'</span></div>' +
      '</div>' +
      '<div class="mcp"><span class="pin">↓ '+(b.ppsIn||0)+'/s</span><span class="pout">↑ '+(b.ppsOut||0)+'/s</span></div>' +
      '<div class="mca">' +
        (!isOn&&!isCon?'<button class="btn sc sm" data-start="'+esc(b.id)+'">▶</button>':'') +
        (isOn||isCon?'<button class="btn dr sm" data-stop="'+esc(b.id)+'">■</button>':'') +
        (isOn||isCon?'<button class="btn wn sm" data-rc="'+esc(b.id)+'">↻</button>':'') +
        '<button class="btn sm" data-sel="'+esc(b.id)+'">📋</button>' +
        '<button class="btn dr sm" data-rm="'+esc(b.id)+'">🗑</button>' +
      '</div>' +
    '</div>'
  }).join('')
  g.querySelectorAll('[data-start]').forEach(el=>el.addEventListener('click',()=>socket.emit('startBot',{id:el.dataset.start},r=>showToast(r?.ok?'▶ Khởi động...':'Lỗi: '+(r?.msg||''),r?.ok?'green':'red'))))
  g.querySelectorAll('[data-stop]').forEach(el=>el.addEventListener('click',()=>socket.emit('stopBot',{id:el.dataset.stop},r=>showToast(r?.ok?'■ Đã tắt':'Lỗi',r?.ok?'yellow':'red'))))
  g.querySelectorAll('[data-rc]').forEach(el=>el.addEventListener('click',()=>{socket.emit('reconnect_bot',{id:el.dataset.rc});showToast('↻ Reconnecting...','yellow')}))
  g.querySelectorAll('[data-sel]').forEach(el=>el.addEventListener('click',()=>{selBot(el.dataset.sel);switchTab('logs')}))
  g.querySelectorAll('[data-rm]').forEach(el=>el.addEventListener('click',()=>{if(confirm('Xóa bot '+el.dataset.rm+'?'))socket.emit('removeBot',{id:el.dataset.rm})}))
}

function selBot(id){
  activeId=id
  socket.emit('subscribe',id)
  logQ=[];logLineCount=0
  const lb=$('lbox');if(lb)lb.innerHTML=''
  renderSB()
  const b=getBot(id);if(!b)return
  showBV()
  renderBV(b)
  renderTabContent()
  fetch('/api/proxies').then(r=>r.json()).then(d=>{pxData=d;if(activeTab==='proxy')renderPx()}).catch(()=>{})
}
function switchTab(n){
  activeTab=n
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===n))
  document.querySelectorAll('.tp').forEach(p=>p.classList.toggle('active',p.id==='tab-'+n))
  renderTabContent()
}
function showNS(){$('no-sel').style.display='';$('bv').style.display='none'}
function showBV(){$('no-sel').style.display='none';$('bv').style.display='flex'}

function renderBV(b){
  renderAbar(b);updateStats(b);updatePos(b);updatePkt(b);updateMenuStatus(b)
  const cfg=b.cfg||{}
  if($('cfg-ow'))$('cfg-ow').value=cfg.ownerUsername||''
  if($('cfg-mc'))$('cfg-mc').value=cfg.menuCommand||'/menu'
  if($('cfg-am'))$('cfg-am').checked=!!cfg.autoMenu
  if($('cfg-rs'))$('cfg-rs').checked=!!cfg.respawn
  if($('cfg-up'))$('cfg-up').checked=cfg.useProxy!==false
}
function renderTabContent(){
  switch(activeTab){
    case'inventory':renderInv(invData[activeId]||[]);break
    case'system':if(sysM)renderSys();break
    case'config':renderCC(ccData[activeId]||[]);break
    case'proxy':renderPx();break
    case'manager':renderMgr();break
  }
}
function renderAbar(b){
  const bar=$('abar');if(!bar)return
  const isJump = b.afk === 'jump'
  const isWalk = b.afk === 'walk'
  const isTshard = !!b.tshard
  const isAutoStats = !!b.autoStats
  const isAutoShard = !!b.autoShard
  const isAutoMenu = !!b.cfg?.autoMenu

  const btns=[
    {l:'⌚ Jump AFK',c:'afk',active:isJump},
    {l:'🚶 Walk AFK',c:'wafk',active:isWalk},
    {l:'⚡ Treo Shard',c:'tshard',active:isTshard},
    {l:'📊 Auto Stats',c:'stats',active:isAutoStats},
    {l:'◈ Auto Shard',c:'shard',active:isAutoShard},
    {l:'🗺 Auto Menu',c:'automenu',active:isAutoMenu},
    {l:'📍 TPA',c:'tpa',active:false},
    {l:'↻ Reconnect',c:'reconnect',active:false},
  ]
  bar.innerHTML=btns.map(btn=>{
    const actClass = btn.active ? ' active' : ''
    return '<button class="btn' + actClass + '" data-c="' + btn.c + '">' + btn.l + '</button>'
  }).join('')
  bar.querySelectorAll('[data-c]').forEach(el=>el.addEventListener('click',()=>sendCmd(el.dataset.c)))
}

function updateMenuStatus(b){
  if(!b||activeId!==b.id)return
  const box=$('menu-status-box'),dot=$('menu-dot'),val=$('menu-val')
  if(!box||!dot||!val)return
  const cfg=b.cfg||{}
  if(!cfg.autoMenu){box.style.display='none';return}
  box.style.display=''
  if(b.menuSuccess){
    dot.className='menu-dot success'
    val.textContent='✓ Vào server thành công (thử '+b.menuRetries+' lần)'
    val.style.color='var(--green)'
  }else if(b.state==='ONLINE'){
    dot.className='menu-dot retrying'
    val.textContent='Đang gửi menu... (lần '+(b.menuRetries||0)+'): '+(cfg.menuCommand||'')
    val.style.color='var(--yellow)'
  }else{
    dot.className='menu-dot idle'
    val.textContent='Chờ kết nối...'
    val.style.color='var(--text3)'
  }
}

function updatePkt(b){
  if(!b||activeId!==b.id)return
  const pki=$('pki'),pko=$('pko')
  if(pki)pki.textContent=b.ppsIn??0
  if(pko)pko.textContent=b.ppsOut??0
}
function updateStats(b){
  if(!b||activeId!==b.id)return
  const g=$('sg');if(!g)return
  const sc=stCls(b.state)
  const items=[
    {label:'State',val:b.state,cls:'c-'+sc},
    {label:'Shard',val:b.shard>0?b.shard.toLocaleString():'—',cls:'c-cyan'},
    {label:'Ping',val:b.ping>=0?b.ping+'ms':'—',cls:'c-blue'},
    {label:'Health',val:'❤ '+(b.health??'—'),cls:'c-red'},
    {label:'Food',val:'🍖 '+(b.food??'—'),cls:'c-yellow'},
    {label:'AFK',val:b.afk||'—',cls:b.afk?'c-yellow':''},
    {label:'Reconnects',val:b.reconnects??0,cls:'c-yellow'},
    {label:'Uptime',val:fmtEl(b.loginTime),cls:'c-green'},
    {label:'Proxy',val:b.proxy||'—',cls:b.proxy?'c-purple':''},
    {label:'Version',val:b.version||'—',cls:''},
  ]
  if(g.children.length!==items.length){
    g.innerHTML=items.map(s=>'<div class="sb"><div class="sl">'+s.label+'</div><div class="sv '+s.cls+'" data-s="'+s.label+'">'+esc(String(s.val))+'</div></div>').join('')
  }else{
    items.forEach(s=>{
      const el=g.querySelector('[data-s="'+s.label+'"]');if(!el)return
      const nv=esc(String(s.val));if(el.innerHTML!==nv)el.innerHTML=nv
      const nc='sv '+(s.cls||'').trim();if(el.className!==nc)el.className=nc
    })
  }
}
function updatePos(b){
  if(!b||activeId!==b.id)return
  const c=$('pdc');if(!c)return
  const p=b.position
  if(!p){c.innerHTML='<span style="color:var(--text4)">—</span>';return}
  c.innerHTML='<div class="co"><span class="co-k">X</span><span class="co-v">'+(p.x?.toFixed(2)??'?')+'</span></div><div class="co"><span class="co-k">Y</span><span class="co-v">'+(p.y?.toFixed(2)??'?')+'</span></div><div class="co"><span class="co-k">Z</span><span class="co-v">'+(p.z?.toFixed(2)??'?')+'</span></div>'
}

// BUG 5 FIX: append-only log rendering with DOM trim from top (no innerHTML clear = no flicker)
// Uses DocumentFragment for batch DOM insert, trims oldest nodes when > MAX
function flushLogs(){
  logPend=false
  if(!logQ.length)return
  const box=$('lbox');if(!box){logQ=[];return}
  const as=$('log-as')?.checked!==false
  const atBot=box.scrollHeight-box.scrollTop-box.clientHeight<80
  const frag=document.createDocumentFragment()
  const MAX=800
  for(const e of logQ){
    const div=document.createElement('div')
    div.className='ll '+(e.level||'')+' ll-new'
    const t=new Date(e.time||Date.now()).toLocaleTimeString('vi-VN',{hour:'2-digit',minute:'2-digit',second:'2-digit'})
    const badge=BADGE_LABELS[e.level]||'SYS'
    div.innerHTML='<span class="ll-time">'+t+'</span><span class="ll-badge">'+badge+'</span><span class="ll-msg">'+esc(e.msg||'')+'</span>'
    frag.appendChild(div)
    logLineCount++
  }
  logQ=[]
  const lc=$('log-cnt');if(lc)lc.textContent=logLineCount+' dòng'
  // BUG 5 FIX: trim from top (removeChild) instead of clearing innerHTML — prevents full re-render flicker
  box.appendChild(frag)
  while(box.children.length>MAX)box.removeChild(box.firstChild)
  if(as&&atBot)box.scrollTop=box.scrollHeight
}

const IICO={sword:'⚔️',axe:'🪓',pickaxe:'⛏️',shovel:'🪣',hoe:'🌾',bow:'🏹',shield:'🛡️',helmet:'⛑️',chestplate:'🦺',leggings:'👖',boots:'👟',apple:'🍎',bread:'🍞',fish:'🐟',meat:'🥩',diamond:'💎',emerald:'💚',gold:'🥇',iron:'⚙️',book:'📚',paper:'📄',map:'🗺️',potion:'🧪',tnt:'💥',chest:'📦',amethyst:'🔮'}
function getIco(t){if(!t)return'📦';const tl=t.toLowerCase();for(const[k,v]of Object.entries(IICO))if(tl.includes(k))return v;return'▪️'}
function renderInv(items){
  const g=$('ig'),s=$('inv-sum');if(!g)return
  if(s)s.textContent=items.length+' items'
  if(!items.length){g.innerHTML='<div class="inv-empty">🎒 Túi đồ trống</div>';return}
  g.innerHTML=items.map(i=>'<div class="ii"><div class="isl">#'+i.slot+'</div><div class="iico">'+getIco(i.type)+'</div><div class="idet"><div class="inm">'+esc(i.name)+'</div><div class="ict">×'+i.count+'</div><div class="ity">'+esc(i.type||'')+'</div></div></div>').join('')
}

function bh(p){const c=p>=85?'danger':p>=60?'warn':'safe';return'<div class="bar-w"><div class="bar"><div class="bf '+c+'" style="width:'+Math.min(100,p)+'%"></div></div><div class="bar-p">'+p+'%</div></div>'}
function sb(l,v,s,b){return'<div class="sys-b"><div class="sys-l">'+l+'</div><div class="sys-v">'+esc(String(v))+'</div>'+(s?'<div class="sys-s">'+esc(String(s))+'</div>':'')+(b!==undefined?bh(b):'')+'</div>'}
function renderSys(){
  const m=sysM;if(!m)return
  const mp=m.memPercent||0,pp=m.procHeapTotal>0?Math.round((m.procHeap/m.procHeapTotal)*100):0
  const sm=$('sys-mem');if(sm)sm.innerHTML=sb('RAM Tổng',fmtMem(m.totalMem),'',undefined)+sb('RAM Dùng',fmtMem(m.usedMem),'của '+fmtMem(m.totalMem),mp)+sb('RAM Trống',fmtMem(m.freeMem),'',undefined)
  const sc=$('sys-cpu');if(sc)sc.innerHTML=sb('CPU Cores',m.cpuCount,(m.cpuModel||'').substring(0,30),undefined)+sb('Load 1m',m.loadAvg?.[0]?.toFixed(2)??'—','',undefined)+sb('Load 5m',m.loadAvg?.[1]?.toFixed(2)??'—','',undefined)+sb('Uptime',fmtUp(m.uptime),'hệ thống',undefined)
  const sp=$('sys-proc');if(sp)sp.innerHTML=sb('Heap',fmtMem(m.procHeap),'của '+fmtMem(m.procHeapTotal),pp)+sb('RSS',fmtMem(m.procRss),'',undefined)
  const si=$('sys-info');const env=srvEnv||{}
  const en=env.isTermux?'Termux':env.isDocker?'Docker':m.platform==='win32'?'Windows':m.platform==='linux'?'Linux':m.platform==='darwin'?'macOS':m.platform||'?'
  if(si)si.innerHTML=sb('Node.js',m.nodeVersion,'',undefined)+sb('Platform',m.platform+' '+m.arch,'',undefined)+sb('Env',en,env.isLowResource?'⚠ Low resource':'',undefined)+(env.autoExe?sb('Mode','⬡ Auto-EXE','Web-only mode',undefined):'')
}
function renderCC(cmds){
  const l=$('ccl');if(!l)return
  if(!cmds.length){l.innerHTML='<div style="color:var(--text3);font-size:11px;padding:3px 0">Chưa có custom command</div>';return}
  l.innerHTML=cmds.map(c=>'<div class="cli"><div class="cln">'+esc(c.name)+'</div><div class="cla">→</div><div class="clv">'+esc(c.cmd)+'</div><div class="cld" data-d="'+esc(c.name)+'">✕</div></div>').join('')
  l.querySelectorAll('[data-d]').forEach(el=>el.addEventListener('click',()=>socket.emit('delCustomCmd',{id:activeId,name:el.dataset.d})))
}
function renderPx(){
  const l=$('px-list'),s=$('px-sum');if(!l)return
  if(s)s.textContent=pxData.length+' proxy'
  if(!pxData.length){l.innerHTML='<div style="color:var(--text3);font-size:11px;padding:9px">Chưa có proxy nào</div>';return}
  l.innerHTML=pxData.map((p,i)=>'<div class="pi"><div class="pidx">'+i+'</div><div class="ptyp">'+esc(p.type)+'</div><div class="pho">'+esc(p.host+':'+p.port)+'</div>'+(p.user?'<span style="color:var(--green);font-size:10px">🔐</span>':'')+'<div class="pdel" data-i="'+i+'">✕</div></div>').join('')
  l.querySelectorAll('[data-i]').forEach(el=>el.addEventListener('click',()=>{
    fetch('/api/proxies/'+el.dataset.i,{method:'DELETE'}).then(r=>r.json()).then(()=>fetch('/api/proxies').then(r=>r.json())).then(d=>{pxData=d;renderPx()}).catch(()=>{})
  }))
}

$('tabs').addEventListener('click',e=>{const t=e.target.closest('.tab');if(!t)return;const n=t.dataset.tab;if(n)switchTab(n)})

function sendCmd(cmd){if(!activeId||!cmd.trim())return;socket.emit('cmd',{id:activeId,cmd:cmd.trim()})}
$('btn-send').addEventListener('click',()=>{const i=$('ci');sendCmd(i.value);i.value=''})
$('ci').addEventListener('keydown',e=>{if(e.key==='Enter'){sendCmd($('ci').value);$('ci').value=''}})
$('btn-lc').addEventListener('click',()=>{
  const b=$('lbox')
  if(b){
    b.innerHTML=''  // explicit clear button — intentional full reset
    logLineCount=0
    const l=$('log-cnt');if(l)l.textContent='0 dòng'
  }
})
$('btn-inv-ref').addEventListener('click',()=>{if(!activeId)return;sendCmd('inv');socket.emit('subscribe',activeId)})
$('btn-sc').addEventListener('click',()=>{
  if(!activeId)return
  const data={ownerUsername:$('cfg-ow').value.trim(),menuCommand:$('cfg-mc').value.trim(),autoMenu:$('cfg-am').checked,respawn:$('cfg-rs').checked,useProxy:$('cfg-up').checked}
  fetch('/api/bots/'+activeId,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}).then(r=>r.json()).then(d=>{if(d.ok)showToast('✓ Đã lưu','green')}).catch(()=>showToast('Lỗi','red'))
})
$('btn-ac').addEventListener('click',()=>{
  const n=$('nc-n').value.trim(),c=$('nc-v').value.trim()
  if(!n||!c){showToast('Điền tên và lệnh','red');return}
  socket.emit('addCustomCmd',{id:activeId,name:n,cmd:c},r=>{if(r?.ok){$('nc-n').value='';$('nc-v').value='';showToast('✓ Đã thêm','green')}})
})
$('btn-stop').addEventListener('click',()=>{if(!activeId||!confirm('Tắt bot '+activeId+'?'))return;socket.emit('stopBot',{id:activeId})})
$('btn-rm').addEventListener('click',()=>{if(!activeId||!confirm('XÓA bot '+activeId+'?'))return;socket.emit('removeBot',{id:activeId})})
$('btn-pxo').addEventListener('click',()=>{const r=$('pxr');r.style.display=r.style.display==='none'?'flex':'none'})
$('btn-pxc').addEventListener('click',()=>{$('pxr').style.display='none'})
$('btn-pxa').addEventListener('click',()=>{
  const v=$('px-val').value.trim();if(!v)return
  fetch('/api/proxies',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy:v})}).then(r=>r.json()).then(d=>{
    if(d.ok){$('px-val').value='';$('pxr').style.display='none';return fetch('/api/proxies').then(r=>r.json())}else throw new Error(d.msg)
  }).then(d=>{pxData=d;renderPx();showToast('✓ Đã thêm proxy','green')}).catch(e=>showToast(e.message||'Lỗi','red'))
})

const openAdd=()=>$('modal-add').classList.add('open')
$('btn-add-open').addEventListener('click',openAdd)
$('btn-add-open2').addEventListener('click',openAdd)
$('btn-mc').addEventListener('click',()=>$('modal-add').classList.remove('open'))
$('btn-mcan').addEventListener('click',()=>$('modal-add').classList.remove('open'))
$('modal-add').addEventListener('click',e=>{if(e.target===$('modal-add'))$('modal-add').classList.remove('open')})
$('btn-msub').addEventListener('click',()=>{
  const id=$('nb-id').value.trim(),u=$('nb-u').value.trim(),h=$('nb-h').value.trim(),p=$('nb-p').value.trim()
  const pw=$('nb-pw').value.trim(),v=$('nb-v').value.trim(),px=$('nb-px').value.trim()
  const er=$('merr')
  if(!id||!h||!p||!u){er.textContent='Vui lòng điền: ID, Host, Port, Username';er.style.display='block';return}
  er.style.display='none'
  const data={id,host:h,port:p,username:u};if(pw)data.password=pw;if(v)data.version=v;if(px)data.proxyIdx=px
  socket.emit('addBot',data,r=>{
    if(r?.ok){
      $('modal-add').classList.remove('open')
      ;['nb-id','nb-u','nb-h','nb-p','nb-pw','nb-v','nb-px'].forEach(id=>{const el=$(id);if(el)el.value=''})
      showToast('✓ Bot đã thêm','green');switchTab('manager')
    }else{er.textContent=r?.msg||'Lỗi';er.style.display='block'}
  })
})
$('btn-sa').addEventListener('click',()=>{
  const off=bots.filter(b=>!['ONLINE','RECONNECTING','CONNECTING','SPAWNING','AUTHENTICATING'].includes(b.state))
  if(!off.length){showToast('Tất cả đang chạy','yellow');return}
  let done=0
  off.forEach(b=>socket.emit('startBot',{id:b.id},r=>{if(r?.ok)done++;if(done===off.length)showToast('▶ Khởi động '+done+' bot','green')}))
})
$('btn-xa').addEventListener('click',()=>{
  if(!confirm('Tắt tất cả bot?'))return
  bots.forEach(b=>socket.emit('stopBot',{id:b.id}))
  showToast('■ Đã tắt tất cả','yellow')
})

function showToast(msg,type='green'){
  const t=$('toast');if(!t)return
  t.textContent=msg;t.className='toast '+type
  setTimeout(()=>t.classList.add('show'),10)
  clearTimeout(t._t);t._t=setTimeout(()=>t.classList.remove('show'),2800)
}

setInterval(()=>{
  const t=$('hdr-time');if(t)t.textContent=new Date().toLocaleTimeString('vi-VN')
  if(activeId&&activeTab==='overview'){
    const b=getBot(activeId)
    if(b){const el=document.querySelector('[data-s="Uptime"]');if(el)el.textContent=fmtEl(b.loginTime)}
  }
},1000)
</script>
</body>
</html>`
  }
}

class BotManager {
  constructor() {
    this.bots = []
    this.proxyManager = new ProxyManager()
    this.ui = new UIManager()
    this.dashboard = null
    this.activeId = null
    this._shuttingDown = false
  }

  init() {
    if (!Array.isArray(config.bots) || !config.bots.length) {
      console.error('config.json cần mảng "bots" với ít nhất 1 entry.')
      process.exit(1)
    }
    if (Array.isArray(config.proxies)) {
      for (const p of config.proxies) this.proxyManager.add(String(p))
    }
    config.bots.forEach((cfg, i) => {
      cfg.id = cfg.id || `bot-${i + 1}`
      cfg.host = cfg.host || config.host
      cfg.port = cfg.port || config.port
      cfg.version = cfg.version || config.version
      cfg.ownerUsername = cfg.ownerUsername || config.ownerUsername
      cfg.botPassword = cfg.botPassword || config.botPassword
      cfg.registered = cfg.registered !== undefined ? cfg.registered : config.registered
      cfg.respawn = cfg.respawn !== undefined ? cfg.respawn : config.respawn
      cfg.useProxy = cfg.useProxy !== undefined ? cfg.useProxy : (config.useProxy !== false)
      cfg.autoMenu = cfg.autoMenu !== undefined ? cfg.autoMenu : (config.autoMenu || false)
      cfg.menuCommand = cfg.menuCommand || config.menuCommand || '/menu'
      const b = this._createBot(cfg, i)
      this.bots.push(b)
    })
    this.activeId = this.bots[0].cfg.id
    this.dashboard = new WebDashboard(this.bots, this.proxyManager)
    if (config.webDashboard !== false) this.dashboard.start()
    return this
  }

  _createBot(cfg, i) {
    const b = new Bot(cfg, pickTheme(cfg.theme, i), this.proxyManager)
    if (cfg.proxyIndex !== undefined) {
      const idx = parseInt(cfg.proxyIndex, 10)
      if (!isNaN(idx) && this.proxyManager.list[idx]) b.proxy = this.proxyManager.list[idx]
    }
    b.on('log', ({ level, id, msg, theme, box }) => {
      if (box) this.ui.printLine(box)
      else this.ui.log(level, id, msg, theme)
    })
    b.on('shard', ({ prev, now }) => this.ui.printShardDiff(b, now, prev))
    b.on('status', () => this.ui.printStatus(b))
    b.on('stateChange', ({ prev, now }) => { if (io) io.emit('botState', { id: cfg.id, prev, state: now }) })
    return b
  }

  _createBotFromData({ id, host, port, username, password, version, proxyIdx }) {
    const base = this.bots[0]?.cfg || config.bots?.[0] || config
    const cfg = {
      id, host, port: parseInt(port, 10), username,
      botPassword: password || '', registered: false,
      version: version || base.version, ownerUsername: base.ownerUsername,
      respawn: false, useProxy: true,
      autoMenu: base.autoMenu || false,
      menuCommand: base.menuCommand || '/menu',
    }
    const b = this._createBot(cfg, this.bots.length)
    if (proxyIdx !== undefined) {
      const idx = parseInt(proxyIdx, 10)
      if (!isNaN(idx) && this.proxyManager.list[idx]) b.proxy = this.proxyManager.list[idx]
    }
    return b
  }

  addBot(args) {
    const [id, host, port, username, password, version, proxyIdx] = args
    if (!id || !host || !port || !username) {
      this.ui.printLine(`${ts()} ${badge('warn')} ${chalk.rgb(255, 200, 0)('Cú pháp: addbot <id> <host> <port> <user> [pass] [ver] [proxy_idx]')}`)
      return
    }
    if (this._findBot(id)) { this.ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)(`ID "${id}" đã tồn tại`)}`); return }
    const b = this._createBotFromData({ id, host, port, username, password, version, proxyIdx })
    this.bots.push(b); b.start()
    this.ui.printLine(`${ts()} ${badge('ok')} ${chalk.rgb(50, 225, 140)(`Đã thêm bot ${chalk.bold(id)} → ${host}:${port}`)}`)
  }

  _findBot(id) { return this.bots.find(b => b.cfg.id.toLowerCase() === (id || '').toLowerCase()) }
  get activeBot() { return this._findBot(this.activeId) }

  handleCommand(line) {
    if (!line) return
    const parts = line.trim().split(/\s+/)
    const key = parts[0].toLowerCase()
    const rest = parts.slice(1)
    const warnNoBot = id => this.ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)(`Bot "${id || ''}" không tồn tại`)}`)

    switch (key) {
      case 'help': {
        const mkTable = (title, data) => {
          const t = new Table({
            head: [chalk.rgb(255, 200, 0).bold('LỆNH'), chalk.rgb(0, 205, 155).bold('MÔ TẢ')],
            colWidths: [26, 46], style: { head: [], border: [], 'padding-left': 1, 'padding-right': 1 }, chars: TABLE_CHARS,
          })
          data.forEach(([cmd, desc]) => t.push([chalk.rgb(255, 200, 0).bold(cmd), chalk.rgb(0, 205, 155)(desc)]))
          return createRoundBox(t.toString(), chalk.cyan, title)
        }
        this.ui.printLine(mkTable('LỆNH HỆ THỐNG', [
          ['use <id>', 'Chuyển bot điều khiển'], ['list', 'Danh sách tất cả bot'],
          ['addbot ...', 'Thêm bot mới'], ['all <cmd>', 'Gửi lệnh tới tất cả bot'],
          ['reconnect [id]', 'Force reconnect'], ['dashboard', 'In URL Web Dashboard'], ['exit', 'Thoát'],
        ]))
        this.ui.printLine(mkTable('LỆNH BOT', [
          ['stats', 'Mở /stats'], ['shard', 'Đọc Shard'], ['afk', 'Jump AFK'],
          ['wafk', 'Walk AFK'], ['stop', 'Tắt AFK'], ['tpa', 'TPA owner'],
          ['menu', 'Gửi menu thủ công'], ['ping', 'Hiện ping'], ['pos', 'Tọa độ'],
          ['inv', 'Xem túi đồ'], ['addcmd <n> <cmd>', 'Thêm custom cmd'], ['delcmd <n>', 'Xóa custom cmd'],
        ]))
        break
      }
      case 'list': this.ui.printList(this.bots); break
      case 'exit': this.gracefulShutdown(); break
      case 'addbot': this.addBot(rest); break
      case 'use': {
        const t = this._findBot(rest[0])
        if (!t) { warnNoBot(rest[0]); return }
        this.activeId = t.cfg.id
        this.ui.printLine(`${ts()} ${badge('ok')} ${chalk.rgb(...t.theme.accent).bold(`Điều khiển → ${t.cfg.id}`)}`)
        break
      }
      case 'all': {
        const c = rest.join(' ')
        if (!c) { this.ui.printLine(`${ts()} ${badge('warn')} ${chalk.rgb(225, 170, 10)('Cú pháp: all <cmd>')}`); return }
        for (const b of this.bots) b.cmd(c)
        break
      }
      case 'status': {
        const t = rest[0] ? this._findBot(rest[0]) : this.activeBot
        if (!t) { warnNoBot(rest[0] || this.activeId); return }
        this.ui.printStatus(t); break
      }
      case 'reconnect': {
        const t = rest[0] ? this._findBot(rest[0]) : this.activeBot
        if (!t) { warnNoBot(rest[0] || this.activeId); return }
        t.forceReconnect(); break
      }
      case 'dashboard':
        this.ui.printLine(`${ts()} ${badge('sys')} ${chalk.rgb(0, 200, 255)(`Web Dashboard: http://localhost:${settings.webPort}`)}`)
        break
      case 'addproxy': { const r = this.proxyManager.add(rest.join(' ')); this.ui.printLine(`${ts()} ${badge(r.ok ? 'proxy' : 'err')} ${chalk.rgb(r.ok ? 160 : 225, r.ok ? 110 : 80, r.ok ? 255 : 80)(r.msg)}`); break }
      case 'loadproxy': {
        const file = rest[0]; if (!file) return
        const r = this.proxyManager.loadFile(path.isAbsolute(file) ? file : path.join(process.cwd(), file))
        if (r.ok) this.ui.printLine(`${ts()} ${badge('proxy')} ${chalk.rgb(160, 110, 255)(`Load: +${r.added} proxy`)}`)
        else this.ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)(r.msg)}`)
        break
      }
      case 'listproxy': this.ui.printProxyList(this.proxyManager); break
      case 'delproxy': {
        const p = this.proxyManager.remove(parseInt(rest[0], 10))
        if (p) this.ui.printLine(`${ts()} ${badge('proxy')} ${chalk.rgb(160, 110, 255)(`Đã xóa ${p.host}:${p.port}`)}`)
        else this.ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)('Index không hợp lệ')}`)
        break
      }
      case 'setproxy': {
        const [bid, pidxStr] = rest; const bot = this._findBot(bid)
        if (!bot) { warnNoBot(bid); return }
        if (pidxStr === 'none') { bot.proxy = null; this.ui.printLine(`${ts()} ${badge('proxy')} Proxy tắt`) }
        else if (pidxStr === 'auto') { bot.proxy = null; this.ui.printLine(`${ts()} ${badge('proxy')} Auto-proxy`) }
        else {
          const idx = parseInt(pidxStr, 10)
          if (isNaN(idx) || !this.proxyManager.list[idx]) { this.ui.printLine(`${ts()} ${badge('err')} Index không hợp lệ`); return }
          bot.proxy = this.proxyManager.list[idx]
          this.ui.printLine(`${ts()} ${badge('proxy')} Bot ${bid} → ${bot.proxy.host}:${bot.proxy.port}`)
        }
        break
      }
      default: {
        const target = this._findBot(key)
        if (target && rest.length) { target.cmd(rest.join(' ')); return }
        const cur = this.activeBot
        if (!cur) { this.ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)('Chưa chọn bot')}`); return }
        cur.cmd(parts.join(' '))
      }
    }
  }

  gracefulShutdown() {
    if (this._shuttingDown) return
    this._shuttingDown = true
    if (!AUTO_EXE) this.ui.printLine(`${ts()} ${badge('sys')} ${chalk.rgb(110, 110, 170)('Đang tắt...')}`)
    else console.log('[Mine Bot] Đang tắt...')
    for (const b of this.bots) try { b.shutdown() } catch { }
    if (expressServer) try { expressServer.close() } catch { }
    setTimeout(() => {
      try { if (typeof rl !== 'undefined' && rl && !rl.closed) rl.close() } catch { }
      process.exit(0)
    }, 3000)
  }

  startAll() { for (const b of this.bots) b.start() }
}

const manager = new BotManager().init()
const ui = manager.ui

if (!AUTO_EXE) {
  ui.printBanner(manager.bots)
}

manager.startAll()

let rl = null
if (!AUTO_EXE) {
  rl = readline.createInterface({
    input: process.stdin, output: process.stdout,
    prompt: chalk.dim.rgb(100, 100, 160)('❱ '), terminal: true,
  })
  ui.setReadline(rl)
  rl.prompt()
  rl.on('line', raw => { manager.handleCommand(raw.trim()); if (!rl.closed) rl.prompt() })
  rl.on('SIGINT', () => manager.gracefulShutdown())
}

process.on('SIGINT', () => manager.gracefulShutdown())
process.on('SIGTERM', () => manager.gracefulShutdown())

process.on('uncaughtException', err => {
  const m = err?.message || String(err)
  if (IGNORED_ERRORS.some(s => m.includes(s))) return
  if (!AUTO_EXE) ui.printLine(`${ts()} ${badge('err')} ${chalk.rgb(225, 80, 80)('[EXCEPTION] ' + m)}`)
  else console.error('[EXCEPTION]', m)
})
process.on('unhandledRejection', r => {
  const m = r?.message || String(r)
  if (IGNORED_ERRORS.some(s => m.includes(s))) return
  if (!AUTO_EXE) ui.printLine(`${ts()} ${badge('warn')} ${chalk.rgb(225, 170, 10)('[REJECTION] ' + m)}`)
  else console.warn('[REJECTION]', m)
})