'use strict';
/**
 * 会員一括管理システム（外部公開用の単体版）
 * - Node.js 標準ライブラリのみ（外部依存なし）
 * - データは JSON ファイルに原子書き込み（tmp -> rename）で保存
 * - 本部パスワードは crypto.scrypt でハッシュ化
 * - リーダーはトークン付き専用リンク（パスワードなし）
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'data.json');
const ROOT_ID = 'COMPANY';

fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------- データストア ----------
function defaultStore() {
  return {
    settings: { hqId: null, hqPassSalt: null, hqPassHash: null },
    members: {
      [ROOT_ID]: { memberId: ROOT_ID, name: '会社', regDate: '', parentId: null, side: null, root: true, enteredBy: 'system', createdAt: new Date().toISOString() }
    },
    leaders: [],   // {id, name, rootMemberId, token, status:'active'|'revoked', consentAt, createdAt}
    conflicts: []  // {id, kind:'update'|'position', memberId, claimed, existing, submittedByName, status:'pending'|'adopted'|'rejected', createdAt, resolvedAt}
  };
}
let store;
function loadStore() {
  if (fs.existsSync(DATA_FILE)) {
    try { store = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); return; }
    catch (e) { console.error('data.json の読み込みに失敗しました。バックアップを確認してください:', e.message); process.exit(1); }
  }
  store = defaultStore();
  saveStore();
}
let saveQueue = Promise.resolve();
function saveStore() {
  const snapshot = JSON.stringify(store, null, 2);
  saveQueue = saveQueue.then(() => {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, snapshot, 'utf8');
    if (fs.existsSync(DATA_FILE)) { try { fs.copyFileSync(DATA_FILE, DATA_FILE + '.bak'); } catch (_) {} }
    fs.renameSync(tmp, DATA_FILE);
  });
  return saveQueue;
}

// ---------- ユーティリティ ----------
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function newToken() { return crypto.randomBytes(24).toString('base64url'); }
function newId(prefix) { return prefix + '_' + crypto.randomBytes(8).toString('hex'); }
function hashPassword(pw) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(pw, salt, 64).toString('hex');
  return { salt, hash };
}
function verifyPassword(pw, salt, hash) {
  if (!salt || !hash) return false;
  const calc = crypto.scryptSync(pw, salt, 64);
  const orig = Buffer.from(hash, 'hex');
  return calc.length === orig.length && crypto.timingSafeEqual(calc, orig);
}
const MEMBER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
function validDate(s) {
  if (!DATE_RE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d) && d.toISOString().slice(0, 10) === s;
}

// ---------- ツリー計算 ----------
function childrenMap() {
  const map = new Map(); // parentId -> {L: member|null, R: member|null}
  for (const m of Object.values(store.members)) {
    if (m.parentId) {
      if (!map.has(m.parentId)) map.set(m.parentId, { L: null, R: null });
      map.get(m.parentId)[m.side] = m;
    }
  }
  return map;
}
function subtreeIds(rootId) {
  const map = childrenMap();
  const out = new Set();
  const stack = [rootId];
  while (stack.length) {
    const id = stack.pop();
    if (out.has(id)) continue;
    out.add(id);
    const ch = map.get(id);
    if (ch) { if (ch.L) stack.push(ch.L.memberId); if (ch.R) stack.push(ch.R.memberId); }
  }
  return out;
}
function countsFor(memberId) {
  const map = childrenMap();
  const ch = map.get(memberId) || { L: null, R: null };
  const left = ch.L ? subtreeIds(ch.L.memberId).size : 0;
  const right = ch.R ? subtreeIds(ch.R.memberId).size : 0;
  return { left, right, total: left + right };
}
function findChild(parentId, side) {
  for (const m of Object.values(store.members)) {
    if (m.parentId === parentId && m.side === side) return m;
  }
  return null;
}
function sameContent(a, b) {
  return a.name === b.name && a.parentId === b.parentId && a.side === b.side && a.regDate === b.regDate;
}

// ---------- 会員追加（本部・リーダー共通ロジック） ----------
// actor: {type:'hq'} | {type:'leader', leader}
// 戻り値: {result:'created'|'duplicate'|'conflict'|'error', message}
async function addMember(actor, fields) {
  const memberId = String(fields.memberId || '').trim();
  const name = String(fields.name || '').trim();
  const parentId = String(fields.parentId || '').trim();
  const side = String(fields.side || '').trim();
  const regDate = String(fields.regDate || '').trim();
  const byName = actor.type === 'hq' ? '本部' : ('リーダー:' + actor.leader.name);

  if (!MEMBER_ID_RE.test(memberId)) return { result: 'error', message: '会員IDは半角英数・ハイフン・アンダースコアで入力してください。' };
  if (memberId === ROOT_ID) return { result: 'error', message: 'その会員IDは使えません。' };
  if (!name || name.length > 100) return { result: 'error', message: '名前を入力してください（100文字まで）。' };
  if (!validDate(regDate)) return { result: 'error', message: '登録日は YYYY-MM-DD 形式の正しい日付で入力してください。' };
  if (side !== 'L' && side !== 'R') return { result: 'error', message: '位置は左か右を選んでください。' };
  const parent = store.members[parentId];
  if (!parent) return { result: 'error', message: '上に付く人（親）の会員IDが見つかりません。' };

  if (actor.type === 'leader') {
    if (!actor.leader.consentAt) return { result: 'error', message: '同意が完了していません。' };
    const scope = subtreeIds(actor.leader.rootMemberId);
    if (!scope.has(parentId)) return { result: 'error', message: '自分の配下の範囲外には追加できません。' };
  }

  const claimed = { memberId, name, parentId, side, regDate };
  const existing = store.members[memberId];
  if (existing) {
    if (sameContent(existing, claimed)) return { result: 'duplicate', message: '同じ内容で登録済みのため、変更はありませんでした。' };
    pushConflict('update', claimed, { ...existing }, byName);
    await saveStore();
    return { result: 'conflict', message: '同じ会員IDで違う内容のため、本部の確認リストに送りました。現在の登録はそのままです。' };
  }
  const occupant = findChild(parentId, side);
  if (occupant) {
    pushConflict('position', claimed, { ...occupant, note: 'その位置は既にこの会員が入っています' }, byName);
    await saveStore();
    return { result: 'conflict', message: 'その位置は既に埋まっているため、本部の確認リストに送りました。' };
  }
  store.members[memberId] = { ...claimed, enteredBy: byName, createdAt: new Date().toISOString() };
  await saveStore();
  return { result: 'created', message: '登録しました。' };
}
function pushConflict(kind, claimed, existingSnap, byName) {
  const dup = store.conflicts.find(c => c.status === 'pending' && c.memberId === claimed.memberId &&
    JSON.stringify(c.claimed) === JSON.stringify(claimed));
  if (dup) return dup;
  const c = { id: newId('cf'), kind, memberId: claimed.memberId, claimed, existing: existingSnap, submittedByName: byName, status: 'pending', createdAt: new Date().toISOString(), resolvedAt: null };
  store.conflicts.push(c);
  return c;
}

// ---------- HTTP 共通 ----------
const sessions = new Map(); // sid -> {hqId, createdAt}
function parseCookies(req) {
  const out = {};
  const raw = req.headers.cookie || '';
  for (const part of raw.split(';')) {
    const i = part.indexOf('=');
    if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}
function isHq(req) {
  const sid = parseCookies(req).sid;
  return sid && sessions.has(sid);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => { data += chunk; if (data.length > 1e6) req.destroy(); });
    req.on('end', () => resolve(new URLSearchParams(data)));
    req.on('error', reject);
  });
}
function send(res, status, html) {
  res.writeHead(status, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
}
function redirect(res, to, setCookie) {
  const headers = { Location: to };
  if (setCookie) headers['Set-Cookie'] = setCookie;
  res.writeHead(302, headers);
  res.end();
}
function page(title, body, flash) {
  const flashHtml = flash && flash.msg ? `<div class="flash ok">${esc(flash.msg)}</div>` : '';
  const errHtml = flash && flash.err ? `<div class="flash ng">${esc(flash.err)}</div>` : '';
  return `<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(title)} | 会員一括管理システム</title><style>
body{font-family:-apple-system,'Hiragino Sans','Noto Sans JP',sans-serif;margin:0;background:#f6f7f9;color:#222}
header{background:#14532d;color:#fff;padding:12px 16px}header a{color:#fff}
main{max-width:960px;margin:0 auto;padding:16px}
.card{background:#fff;border:1px solid #ddd;border-radius:10px;padding:16px;margin:12px 0}
input,select{padding:8px;border:1px solid #bbb;border-radius:6px;margin:4px 0;width:100%;box-sizing:border-box}
button,.btn{display:inline-block;background:#14532d;color:#fff;border:0;padding:9px 16px;border-radius:6px;cursor:pointer;text-decoration:none}
button.sub,.btn.sub{background:#555}button.danger{background:#b91c1c}
.flash{padding:10px;border-radius:6px;margin:10px 0}.flash.ok{background:#dcfce7}.flash.ng{background:#fee2e2}
table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:6px 8px;font-size:14px;text-align:left}
ul.tree{list-style:none;padding-left:18px;border-left:2px solid #bbf7d0;margin:4px 0}
ul.tree li{margin:4px 0}.node{background:#fff;border:1px solid #ccc;border-radius:8px;padding:6px 10px;display:inline-block}
.root-node{background:#14532d;color:#fff;font-weight:bold}
.small{font-size:12px;color:#555}.nav a{margin-right:12px}
</style></head><body><header><strong>会員一括管理システム</strong></header><main>${flashHtml}${errHtml}${body}</main></body></html>`;
}
function flashFrom(url) {
  return { msg: url.searchParams.get('msg') || '', err: url.searchParams.get('err') || '' };
}
function back(to, key, text) { return `${to}?${key}=${encodeURIComponent(text)}`; }

// ---------- ツリー描画 ----------
function renderTree(rootId, maxDepth = 30, depth = 0) {
  const m = store.members[rootId];
  if (!m) return '';
  const c = countsFor(rootId);
  const label = m.root
    ? `<span class="node root-node">${esc(m.name)}</span>`
    : `<span class="node"><strong>${esc(m.name)}</strong> <span class="small">ID:${esc(m.memberId)} / 登録:${esc(m.regDate || '-')} / 左${c.left}・右${c.right}</span></span>`;
  if (depth >= maxDepth) return `<li>${label} <span class="small">（これより下は会員一覧で確認してください）</span></li>`;
  const map = childrenMap();
  const ch = map.get(rootId);
  let childrenHtml = '';
  if (ch && (ch.L || ch.R)) {
    const l = ch.L ? renderTree(ch.L.memberId, maxDepth, depth + 1) : '<li><span class="small">左：空き</span></li>';
    const r = ch.R ? renderTree(ch.R.memberId, maxDepth, depth + 1) : '<li><span class="small">右：空き</span></li>';
    childrenHtml = `<ul class="tree">${l}${r}</ul>`;
  }
  return `<li>${label}${childrenHtml}</li>`;
}
function memberOptions(selectedId, scopeIds) {
  const list = Object.values(store.members).filter(m => !scopeIds || scopeIds.has(m.memberId));
  list.sort((a, b) => a.memberId.localeCompare(b.memberId));
  return list.map(m => `<option value="${esc(m.memberId)}" ${m.memberId === selectedId ? 'selected' : ''}>${esc(m.name)}（${esc(m.memberId)}）</option>`).join('');
}
function addMemberForm(action, scopeIds) {
  return `<form method="post" action="${action}">
  <label>名前<input name="name" required maxlength="100"></label>
  <label>会員ID<input name="memberId" required maxlength="64" placeholder="半角英数・-・_"></label>
  <label>登録日<input name="regDate" required type="date"></label>
  <label>上に付く人（親）<select name="parentId">${memberOptions('', scopeIds)}</select></label>
  <label>位置<select name="side"><option value="L">左</option><option value="R">右</option></select></label>
  <button type="submit">登録する</button></form>`;
}

// ---------- 本部 ----------
function hqNav() {
  return `<p class="nav"><a href="/hq">組織図（全体）</a><a href="/hq/members">会員一覧</a><a href="/hq/leaders">リーダー管理</a><a href="/hq/conflicts">要確認リスト</a><a href="/hq/export.csv">CSV書き出し</a></p>
  <form method="post" action="/logout" style="display:inline"><button class="sub" type="submit">ログアウト</button></form>`;
}
async function handleSetup(req, res, url) {
  if (store.settings.hqPassHash) return redirect(res, '/login');
  if (req.method === 'GET') {
    return send(res, 200, page('本部セットアップ', `<div class="card"><h2>本部の初期設定</h2>
    <p>最初に、本部ログイン用のIDとパスワードを設定してください。</p>
    <form method="post" action="/setup"><label>本部ID<input name="hqId" required></label>
    <label>パスワード（8文字以上）<input name="password" type="password" required minlength="8"></label>
    <button type="submit">設定する</button></form></div>`, flashFrom(url)));
  }
  const body = await readBody(req);
  const hqId = String(body.get('hqId') || '').trim();
  const pw = String(body.get('password') || '');
  if (!hqId || pw.length < 8) return redirect(res, back('/setup', 'err', '本部IDと8文字以上のパスワードを入力してください。'));
  const { salt, hash } = hashPassword(pw);
  store.settings.hqId = hqId; store.settings.hqPassSalt = salt; store.settings.hqPassHash = hash;
  await saveStore();
  return redirect(res, back('/login', 'msg', '本部の初期設定ができました。ログインしてください。'));
}
async function handleLogin(req, res, url) {
  if (!store.settings.hqPassHash) return redirect(res, '/setup');
  if (req.method === 'GET') {
    return send(res, 200, page('本部ログイン', `<div class="card"><h2>本部ログイン</h2>
    <form method="post" action="/login"><label>本部ID<input name="hqId" required></label>
    <label>パスワード<input name="password" type="password" required></label>
    <button type="submit">ログイン</button></form></div>`, flashFrom(url)));
  }
  const body = await readBody(req);
  const hqId = String(body.get('hqId') || '').trim();
  const pw = String(body.get('password') || '');
  if (hqId !== store.settings.hqId || !verifyPassword(pw, store.settings.hqPassSalt, store.settings.hqPassHash)) {
    return redirect(res, back('/login', 'err', 'IDまたはパスワードが違います。'));
  }
  const sid = newToken();
  sessions.set(sid, { hqId, createdAt: Date.now() });
  return redirect(res, '/hq', `sid=${sid}; HttpOnly; Path=/; SameSite=Lax`);
}
async function handleHq(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  const total = Object.keys(store.members).length - 1;
  const pending = store.conflicts.filter(c => c.status === 'pending').length;
  const body = `${hqNav()}
  <div class="card"><h2>組織図（会社全体・1本のツリー）</h2>
  <p>会員数：${total}人 / リーダー：${store.leaders.filter(l => l.status === 'active').length}人 / 要確認：${pending}件</p>
  <ul class="tree">${renderTree(ROOT_ID)}</ul></div>
  <div class="card"><h3>会員を追加（本部）</h3>${addMemberForm('/hq/members/add', null)}</div>`;
  return send(res, 200, page('本部：組織図', body, flashFrom(url)));
}
async function handleHqMembers(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  if (req.method === 'POST') {
    const body0 = await readBody(req);
    const r = await addMember({ type: 'hq' }, Object.fromEntries(body0));
    return redirect(res, back('/hq/members', r.result === 'error' ? 'err' : 'msg', r.message));
  }
  const q = (url.searchParams.get('q') || '').trim().toLowerCase();
  let list = Object.values(store.members).filter(m => !m.root);
  if (q) list = list.filter(m => m.name.toLowerCase().includes(q) || m.memberId.toLowerCase().includes(q));
  list.sort((a, b) => a.memberId.localeCompare(b.memberId));
  const rows = list.map(m => {
    const c = countsFor(m.memberId);
    const parent = m.parentId ? store.members[m.parentId] : null;
    return `<tr><td>${esc(m.memberId)}</td><td>${esc(m.name)}</td><td>${esc(m.regDate || '')}</td><td>${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')}</td><td>${m.side === 'L' ? '左' : m.side === 'R' ? '右' : ''}</td><td>${c.left}</td><td>${c.right}</td><td>${esc(m.enteredBy || '')}</td></tr>`;
  }).join('');
  const body = `${hqNav()}<div class="card"><h2>会員一覧（${list.length}件）</h2>
  <form method="get" action="/hq/members"><input name="q" value="${esc(q)}" placeholder="名前・会員IDで検索"><button type="submit">検索</button></form>
  <table><tr><th>会員ID</th><th>名前</th><th>登録日</th><th>親</th><th>位置</th><th>左人数</th><th>右人数</th><th>入力者</th></tr>${rows || '<tr><td colspan="8">該当なし</td></tr>'}</table></div>
  <div class="card"><h3>会員を追加（本部）</h3>${addMemberForm('/hq/members/add', null)}</div>`;
  return send(res, 200, page('本部：会員一覧', body, flashFrom(url)));
}
async function handleHqLeaders(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  if (req.method === 'POST') {
    const body0 = await readBody(req);
    const action = body0.get('action');
    if (action === 'create') {
      const name = String(body0.get('name') || '').trim();
      const rootMemberId = String(body0.get('rootMemberId') || '').trim();
      if (!name) return redirect(res, back('/hq/leaders', 'err', 'リーダー名を入力してください。'));
      if (!store.members[rootMemberId] || rootMemberId === ROOT_ID) return redirect(res, back('/hq/leaders', 'err', '担当のトップになる会員（ルート会員）を選んでください。'));
      store.leaders.push({ id: newId('ldr'), name, rootMemberId, token: newToken(), status: 'active', consentAt: null, createdAt: new Date().toISOString() });
      await saveStore();
      return redirect(res, back('/hq/leaders', 'msg', 'リーダーを作成し、専用リンクを発行しました。'));
    }
    const leader = store.leaders.find(l => l.id === body0.get('leaderId'));
    if (!leader) return redirect(res, back('/hq/leaders', 'err', 'リーダーが見つかりません。'));
    if (action === 'reissue') { leader.token = newToken(); leader.status = 'active'; await saveStore(); return redirect(res, back('/hq/leaders', 'msg', '専用リンクを再発行しました（前のリンクは使えません）。')); }
    if (action === 'revoke') { leader.status = 'revoked'; await saveStore(); return redirect(res, back('/hq/leaders', 'msg', '専用リンクを無効化しました。')); }
    return redirect(res, '/hq/leaders');
  }
  const base = `${req.headers['x-forwarded-proto'] ? String(req.headers['x-forwarded-proto']).split(',')[0] : 'http'}://${req.headers.host}`;
  const rows = store.leaders.map(l => {
    const root = store.members[l.rootMemberId];
    const link = l.status === 'active' ? `${base}/leader/${l.token}` : '（無効化済み）';
    return `<tr><td>${esc(l.name)}</td><td>${esc(root ? root.name + '（' + root.memberId + '）' : l.rootMemberId)}</td>
    <td style="word-break:break-all">${esc(link)}</td><td>${l.status === 'active' ? '有効' : '無効'} / ${l.consentAt ? '同意済み' : '未同意'}</td>
    <td><form method="post" action="/hq/leaders" style="display:inline"><input type="hidden" name="leaderId" value="${esc(l.id)}"><input type="hidden" name="action" value="reissue"><button class="sub" type="submit">再発行</button></form>
    <form method="post" action="/hq/leaders" style="display:inline"><input type="hidden" name="leaderId" value="${esc(l.id)}"><input type="hidden" name="action" value="revoke"><button class="danger" type="submit">無効化</button></form></td></tr>`;
  }).join('');
  const body = `${hqNav()}<div class="card"><h2>リーダー管理</h2>
  <p class="small">専用リンクをリーダーに渡してください。リンクを開くとパスワードなしで、そのリーダーの配下だけが見える画面に入ります。再発行すると前のリンクは使えなくなります。</p>
  <table><tr><th>リーダー名</th><th>担当トップ（ルート会員）</th><th>専用リンク</th><th>状態</th><th>操作</th></tr>${rows || '<tr><td colspan="5">まだいません</td></tr>'}</table></div>
  <div class="card"><h3>リーダーを作る</h3><form method="post" action="/hq/leaders">
  <input type="hidden" name="action" value="create">
  <label>リーダー名<input name="name" required></label>
  <label>担当トップ（この会員から下が、そのリーダーの担当範囲です）<select name="rootMemberId">${memberOptions('', null)}</select></label>
  <button type="submit">作成してリンクを発行</button></form>
  <p class="small">※ 担当トップの会員がまだいない場合は、先に「会員一覧」から本部がその会員を登録してください。</p></div>`;
  return send(res, 200, page('本部：リーダー管理', body, flashFrom(url)));
}
async function handleHqConflicts(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  if (req.method === 'POST') {
    const body0 = await readBody(req);
    const c = store.conflicts.find(x => x.id === body0.get('conflictId'));
    if (!c || c.status !== 'pending') return redirect(res, back('/hq/conflicts', 'err', '対象の確認項目が見つかりません。'));
    const action = body0.get('action');
    if (action === 'reject') {
      c.status = 'rejected'; c.resolvedAt = new Date().toISOString();
      await saveStore();
      return redirect(res, back('/hq/conflicts', 'msg', '申告を却下しました（現在の登録が正のままです）。'));
    }
    if (action === 'adopt') {
      const claimed = c.claimed;
      if (c.kind === 'update') {
        const cur = store.members[c.memberId];
        if (!cur) return redirect(res, back('/hq/conflicts', 'err', '対象会員が見つかりません。'));
        if ((claimed.parentId !== cur.parentId || claimed.side !== cur.side)) {
          const occ = findChild(claimed.parentId, claimed.side);
          if (occ && occ.memberId !== c.memberId) return redirect(res, back('/hq/conflicts', 'err', '移動先の位置は既に埋まっているため採用できません。先にそちらを整理してください。'));
        }
        Object.assign(cur, { name: claimed.name, parentId: claimed.parentId, side: claimed.side, regDate: claimed.regDate, enteredBy: c.submittedByName + '（本部が採用）' });
      } else { // position: 新規会員だが位置が埋まっていた
        if (store.members[c.memberId]) return redirect(res, back('/hq/conflicts', 'err', 'その会員IDは既に登録されています。'));
        const occ = findChild(claimed.parentId, claimed.side);
        if (occ) return redirect(res, back('/hq/conflicts', 'err', 'その位置はまだ埋まっているため採用できません。'));
        store.members[c.memberId] = { ...claimed, enteredBy: c.submittedByName + '（本部が採用）', createdAt: new Date().toISOString() };
      }
      c.status = 'adopted'; c.resolvedAt = new Date().toISOString();
      await saveStore();
      return redirect(res, back('/hq/conflicts', 'msg', '申告を採用し、本部データに反映しました。'));
    }
    return redirect(res, '/hq/conflicts');
  }
  const pending = store.conflicts.filter(c => c.status === 'pending');
  const rows = pending.map(c => {
    const parent = store.members[c.claimed.parentId];
    const cur = store.members[c.memberId];
    return `<tr><td>${esc(c.memberId)}</td>
    <td>${cur ? `${esc(cur.name)} / 親:${esc(cur.parentId)} ${cur.side === 'L' ? '左' : '右'} / ${esc(cur.regDate || '')}` : '（未登録）'}</td>
    <td>${esc(c.claimed.name)} / 親:${esc(parent ? parent.name + '（' + parent.memberId + '）' : c.claimed.parentId)} ${c.claimed.side === 'L' ? '左' : '右'} / ${esc(c.claimed.regDate)}</td>
    <td>${esc(c.submittedByName)}<br><span class="small">${esc(c.createdAt.slice(0, 10))}</span></td>
    <td><form method="post" action="/hq/conflicts" style="display:inline"><input type="hidden" name="conflictId" value="${esc(c.id)}"><input type="hidden" name="action" value="adopt"><button type="submit">申告を採用</button></form>
    <form method="post" action="/hq/conflicts" style="display:inline"><input type="hidden" name="conflictId" value="${esc(c.id)}"><input type="hidden" name="action" value="reject"><button class="danger" type="submit">却下</button></form></td></tr>`;
  }).join('');
  const body = `${hqNav()}<div class="card"><h2>要確認リスト（${pending.length}件）</h2>
  <p class="small">同じ会員IDで違う内容が届いたもの、または既に埋まっている位置への登録です。採用するまで現在の登録が正です。</p>
  <table><tr><th>会員ID</th><th>現在の登録</th><th>届いた申告</th><th>申告者</th><th>操作</th></tr>${rows || '<tr><td colspan="5">要確認はありません</td></tr>'}</table></div>`;
  return send(res, 200, page('本部：要確認リスト', body, flashFrom(url)));
}
function handleExportCsv(req, res) {
  if (!isHq(req)) return redirect(res, '/login');
  const list = Object.values(store.members).filter(m => !m.root).sort((a, b) => a.memberId.localeCompare(b.memberId));
  const quote = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const lines = [['会員ID', '名前', '登録日', '親の会員ID', '位置', '左人数', '右人数', '配下合計'].join(',')];
  for (const m of list) {
    const c = countsFor(m.memberId);
    lines.push([m.memberId, m.name, m.regDate || '', m.parentId || '', m.side === 'L' ? '左' : m.side === 'R' ? '右' : '', c.left, c.right, c.total].map(quote).join(','));
  }
  const csv = '﻿' + lines.join('\r\n') + '\r\n';
  res.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': 'attachment; filename="members.csv"' });
  res.end(csv);
}

// ---------- リーダー ----------
function getLeader(token) {
  return store.leaders.find(l => l.token === token && l.status === 'active');
}
const CONSENT_TEXT = `
<div class="card"><h2>情報の取り扱いについて</h2>
<p>この画面は、組織図の作成と会社による会員管理のために使います。</p>
<ul>
<li>集める情報：名前、会員ID、バイナリー上の位置、登録日（連絡先は集めません）</li>
<li>見ることができる人：本部は全体を見ることができます。リーダーは自分の配下だけを見ることができます。一般の会員に公開されることはありません。</li>
<li>同じ会員IDで違う内容が入力された場合は、勝手に上書きされず、本部の確認に回ります。</li>
</ul>
<p>内容を確認し、同意いただける場合は「同意して始める」を押してください。</p></div>`;
async function handleLeader(req, res, url, token) {
  const leader = getLeader(token);
  if (!leader) return send(res, 403, page('リンクが使えません', '<div class="card"><h2>このリンクは使えません</h2><p>リンクが無効化されているか、再発行で変わっている可能性があります。本部に新しいリンクを確認してください。</p></div>'));
  const basePath = '/leader/' + token;
  if (req.method === 'POST' && url.pathname.endsWith('/consent')) {
    leader.consentAt = new Date().toISOString();
    await saveStore();
    return redirect(res, back(basePath, 'msg', '同意ありがとうございました。入力を始められます。'));
  }
  if (!leader.consentAt) {
    if (req.method === 'POST' && url.pathname.endsWith('/add')) {
      return redirect(res, back(basePath, 'err', '同意が完了してから入力してください。'));
    }
    return send(res, 200, page('同意のお願い', `${CONSENT_TEXT}
    <form method="post" action="${basePath}/consent"><button type="submit">同意して始める</button></form>`, flashFrom(url)));
  }
  if (req.method === 'POST' && url.pathname.endsWith('/add')) {
    const body0 = await readBody(req);
    const r = await addMember({ type: 'leader', leader }, Object.fromEntries(body0));
    return redirect(res, back(basePath, r.result === 'error' ? 'err' : 'msg', r.message));
  }
  const scope = subtreeIds(leader.rootMemberId);
  const root = store.members[leader.rootMemberId];
  const body = `<div class="card"><h2>${esc(leader.name)} さんの組織</h2>
  <p class="small">担当トップ：${esc(root ? root.name + '（' + root.memberId + '）' : leader.rootMemberId)} / ここから下だけが表示・入力できます。</p>
  <ul class="tree">${renderTree(leader.rootMemberId)}</ul></div>
  <div class="card"><h3>会員を追加する</h3>${addMemberForm(basePath + '/add', scope)}</div>`;
  return send(res, 200, page('リーダー画面', body, flashFrom(url)));
}

// ---------- ルーティング ----------
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const p = url.pathname;
    if (p === '/') {
      if (!store.settings.hqPassHash) return redirect(res, '/setup');
      const body = `<div class="card"><h2>会員一括管理システム</h2>
      <p>本部の方はログインしてください。リーダーの方は、本部から渡された専用リンクから開いてください。</p>
      <p><a class="btn" href="/login">本部ログイン</a></p></div>`;
      return send(res, 200, page('トップ', body));
    }
    if (p === '/setup') return handleSetup(req, res, url);
    if (p === '/login') return handleLogin(req, res, url);
    if (p === '/logout' && req.method === 'POST') {
      const sid = parseCookies(req).sid;
      if (sid) sessions.delete(sid);
      return redirect(res, '/login', 'sid=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0');
    }
    if (p === '/hq') return handleHq(req, res, url);
    if (p === '/hq/members' || p === '/hq/members/add') return handleHqMembers(req, res, url);
    if (p === '/hq/leaders') return handleHqLeaders(req, res, url);
    if (p === '/hq/conflicts') return handleHqConflicts(req, res, url);
    if (p === '/hq/export.csv') return handleExportCsv(req, res);
    const lm = p.match(/^\/leader\/([A-Za-z0-9_-]+)(\/(consent|add))?$/);
    if (lm) return handleLeader(req, res, url, lm[1]);
    return send(res, 404, page('見つかりません', '<div class="card"><h2>ページが見つかりません</h2></div>'));
  } catch (e) {
    console.error(e);
    return send(res, 500, page('エラー', '<div class="card"><h2>サーバーエラーが発生しました</h2><p>時間をおいて再度お試しください。</p></div>'));
  }
});

loadStore();
if (require.main === module) {
  server.listen(PORT, HOST, () => {
    console.log(`会員一括管理システムを起動しました: http://${HOST}:${PORT} (data: ${DATA_FILE})`);
    if (!store.settings.hqPassHash) console.log('初回セットアップ: /setup で本部IDとパスワードを設定してください。');
  });
}
module.exports = { server, loadStore };
