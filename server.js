'use strict';
/**
 * 会員一括管理システム（外部公開用の単体版）
 * - Node.js 標準ライブラリのみ（外部依存なし）
 * - データの保存先は環境変数で切り替える:
 *   - DATABASE_URL があれば Neon Postgres（HTTP SQL API）に状態一式を保存（無料枠運用向け）
 *   - なければ従来どおり JSON ファイルに原子書き込み（tmp -> rename）で保存
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

// ---------- 保存先の切り替え ----------
// DATABASE_URL が設定されていれば Neon Postgres に保存し、未設定ならローカルファイルに保存する。
const DATABASE_URL = process.env.DATABASE_URL || '';
const DB_MODE = DATABASE_URL.length > 0;
// テスト・検証用に Neon HTTP エンドポイントを上書きできる（通常は DATABASE_URL のホストから導出する）
const NEON_HTTP_ENDPOINT = process.env.NEON_HTTP_ENDPOINT || '';

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
function writeStoreToFile(snapshot) {
  const tmp = DATA_FILE + '.tmp';
  fs.writeFileSync(tmp, snapshot, 'utf8');
  if (fs.existsSync(DATA_FILE)) { try { fs.copyFileSync(DATA_FILE, DATA_FILE + '.bak'); } catch (_) {} }
  fs.renameSync(tmp, DATA_FILE);
}
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
  if (DB_MODE) return saveStoreToDb();
  const snapshot = JSON.stringify(store, null, 2);
  saveQueue = saveQueue.then(() => writeStoreToFile(snapshot));
  return saveQueue;
}

// ---------- Neon Postgres バックエンド ----------
// 状態一式を app_state テーブルの1行（k='state', v=状態JSON）に保存する。
// 通信は Neon の HTTP SQL API: POST https://<DATABASE_URLのホスト>/sql
//   ヘッダー: Neon-Connection-String: <DATABASE_URL>
//   ボディ:   {"query": "...", "params": [...]}
//   レスポンス: {"rows": [...], "fields": [...], "command": "...", "rowCount": n}
const STATE_KEY = 'state';
const SQL_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS app_state (k text PRIMARY KEY, v jsonb)';
const SQL_SELECT_STATE = 'SELECT v FROM app_state WHERE k = $1';
const SQL_UPSERT_STATE = 'INSERT INTO app_state (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v';

function neonEndpoint() {
  if (NEON_HTTP_ENDPOINT) return NEON_HTTP_ENDPOINT;
  const u = new URL(DATABASE_URL); // 形式が不正ならここで例外 → 起動失敗として表面化させる
  return `https://${u.host}/sql`;
}
async function dbQuery(query, params) {
  let res;
  try {
    res = await fetch(neonEndpoint(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Neon-Connection-String': DATABASE_URL },
      body: JSON.stringify({ query, params: params || [] }),
      signal: AbortSignal.timeout ? AbortSignal.timeout(30000) : undefined
    });
  } catch (e) {
    throw new Error('データベース（Neon）に接続できませんでした: ' + e.message);
  }
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch (_) { /* エラー処理側で text を使う */ }
  if (!res.ok) {
    const msg = (json && (json.message || json.error)) || text || ('HTTP ' + res.status);
    throw new Error('データベース（Neon）でエラーが発生しました: ' + msg);
  }
  return json || {};
}

// DBへの保存はプロミスチェーンで直列化し、同時リクエストで書き込みが交錯しないようにする。
// 失敗した保存は呼び出し側（await saveStore() しているハンドラ）へ例外として伝える。
// ただしチェーン自体は失敗で止めず、次の保存は改めて試みる。
let dbSaveQueue = Promise.resolve();
function saveStoreToDb() {
  const snapshot = JSON.stringify(store, null, 2);
  const op = dbSaveQueue.then(async () => {
    // ローカルファイルにもミラー保存する（best effort。本の保存先はDB。
    // ファイルが消える環境でも最後の状態の控えにはなるが、起動時は必ずDBから読む）
    try { writeStoreToFile(snapshot); }
    catch (e) { console.error('ローカルファイルへのミラー保存に失敗しました（DBへの保存は続行します）:', e.message); }
    await dbQuery(SQL_UPSERT_STATE, [STATE_KEY, snapshot]);
  });
  dbSaveQueue = op.catch(() => {});
  return op;
}

// DBモードの起動処理: テーブルを用意し、保存済みの状態を読む。
// 行が無ければファイル版と同じ初期状態（会社枠のみ）を作って挿入する。
// DBに繋がらない・データが壊れている場合は、空の状態で起動して上書きするようなことはせず、例外にする。
async function initDbStore() {
  await dbQuery(SQL_CREATE_TABLE, []);
  const r = await dbQuery(SQL_SELECT_STATE, [STATE_KEY]);
  const row = r && Array.isArray(r.rows) ? r.rows[0] : null;
  if (row && row.v != null) {
    const parsed = typeof row.v === 'string' ? JSON.parse(row.v) : row.v;
    if (!parsed || typeof parsed !== 'object' || !parsed.settings || !parsed.members || !parsed.members[ROOT_ID]) {
      throw new Error('データベースに保存されているデータの形式が不正です（保護のため上書きしません）');
    }
    store = parsed;
    return;
  }
  store = defaultStore();
  await saveStoreToDb();
}

async function boot() {
  if (DB_MODE) await initDbStore();
  else loadStore();
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
function todayJst() {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(new Date());
  const p = {};
  for (const part of parts) p[part.type] = part.value;
  return `${p.year}-${p.month}-${p.day}`;
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
// ルートから配下を幅優先で辿り、会員ごとの「ルートからの深さ（段-1）」を返す。
// リーダー画面の会員検索で、その人の段を表示するために使う。
function scopeDepthMap(rootId) {
  const map = childrenMap();
  const out = new Map();
  const queue = [rootId];
  out.set(rootId, 0);
  for (let i = 0; i < queue.length; i++) {
    const id = queue[i];
    const ch = map.get(id);
    if (!ch) continue;
    for (const side of ['L', 'R']) {
      const child = ch[side];
      if (child && !out.has(child.memberId)) {
        out.set(child.memberId, out.get(id) + 1);
        queue.push(child.memberId);
      }
    }
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

// ---------- 会員IDの自動採番 ----------
// 人が入力しない場合の採番ルール:
// - 既存の数字だけの会員IDの最大値 + 1（文字列）。数字IDが無ければ 1 から。
// - 既存会員・本部の確認待ち申告（pending）が使っているIDは避け、空くまで繰り上げる。
// - 数字でないID（'T1' や 'A-100' のような旧ID）は採番に影響しない。
// - 大きな数字でも桁あふれしないよう BigInt で計算する（'007' のような表記も数値として扱う）。
function nextAutoMemberId() {
  const taken = new Set(Object.keys(store.members));
  for (const c of store.conflicts) {
    if (c.status === 'pending' && c.memberId) taken.add(c.memberId);
  }
  let max = null;
  for (const id of taken) {
    if (/^\d+$/.test(id)) {
      const v = BigInt(id);
      if (max === null || v > max) max = v;
    }
  }
  let n = max === null ? 1n : max + 1n;
  while (taken.has(String(n))) n += 1n;
  return String(n);
}

// ---------- 会員追加（本部・リーダー共通ロジック） ----------
// actor: {type:'hq'} | {type:'leader', leader}
// 戻り値: {result:'created'|'duplicate'|'conflict'|'error', message}
async function addMember(actor, fields) {
  let memberId = String(fields.memberId || '').trim();
  const name = String(fields.name || '').trim();
  const parentId = String(fields.parentId || '').trim();
  const side = String(fields.side || '').trim();
  let regDate = String(fields.regDate || '').trim();
  // 登録日が未入力なら、今日（日本時間）を既定にする（組織図内のフォーム・下のフォーム共通）
  if (!regDate) regDate = todayJst();
  const byName = actor.type === 'hq' ? '本部' : ('リーダー:' + actor.leader.name);

  if (memberId === '') {
    // 会員IDが送られてこなければ、システムが自動で採番する（人が入力する欄はフォームから無くした）
    memberId = nextAutoMemberId();
  } else {
    // 明示的に会員IDが指定された場合は従来どおり検証する（テスト・取り込み用）
    if (!MEMBER_ID_RE.test(memberId)) return { result: 'error', message: '会員IDは半角英数・ハイフン・アンダースコアで入力してください。' };
    if (memberId === ROOT_ID) return { result: 'error', message: 'その会員IDは使えません。' };
  }
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
  return { result: 'created', message: `登録しました。会員IDは ${memberId} です。` };
}
function pushConflict(kind, claimed, existingSnap, byName) {
  const dup = store.conflicts.find(c => c.status === 'pending' && c.memberId === claimed.memberId &&
    JSON.stringify(c.claimed) === JSON.stringify(claimed));
  if (dup) return dup;
  const c = { id: newId('cf'), kind, memberId: claimed.memberId, claimed, existing: existingSnap, submittedByName: byName, status: 'pending', createdAt: new Date().toISOString(), resolvedAt: null };
  store.conflicts.push(c);
  return c;
}

// ---------- 会員情報の修正（リーダー） ----------
// リーダーが自分の担当範囲内にある会員の名前・登録日を直接修正する。
// これは担当リーダーによる訂正であり、新規登録の申告ではないため、本部の要確認リスト
// （conflicts）は作らない。会員ID・親・位置・入力者は変更しない。
async function updateMemberByLeader(leader, fields) {
  if (!leader.consentAt) return { result: 'error', message: '同意が完了していません。' };
  const memberId = String(fields.memberId || '').trim();
  const member = store.members[memberId];
  const scope = subtreeIds(leader.rootMemberId);
  if (!member || !scope.has(memberId)) {
    return { result: 'error', message: '修正できる会員が見つかりません。' };
  }
  const name = String(fields.name || '').trim();
  if (!name || name.length > 100) return { result: 'error', message: '名前を入力してください（100文字まで）。' };
  const submittedDate = String(fields.regDate || '').trim();
  let regDate = member.regDate || '';
  if (submittedDate) {
    if (!validDate(submittedDate)) return { result: 'error', message: '登録日は YYYY-MM-DD 形式の正しい日付で入力してください。' };
    regDate = submittedDate;
  }
  member.name = name;
  member.regDate = regDate;
  await saveStore();
  return { result: 'updated', message: '会員情報を修正しました。' };
}

// ---------- 会員削除（本部・リーダー共通ガード） ----------
// 配置ミスの訂正として会員を削除できるようにするが、組織図が壊れる消し方はできない
// ように、削除してよい条件をここに一本化する。削除できるのは「下に誰もいない末端の
// 会員」だけであり、次のいずれかに当てはまると理由を返して拒否する:
// - 対象の会員が存在しない
// - 会社の枠（トップ）である
// - 下に子（左・右どちらか）がいる（下の会員から先に削除する必要がある）
// - いずれかのリーダーの担当トップになっている（そのリーダーの専用リンクが壊れる）
// - その会員について本部の確認待ち（pending）の申告がある（先に要確認リストで対応する）
function memberDeleteBlockReason(memberId) {
  const member = store.members[memberId];
  if (!member) return '対象の会員が見つかりません。';
  if (memberId === ROOT_ID || member.root) return '会社の枠は削除できません。';
  const ch = childrenMap().get(memberId) || { L: null, R: null };
  if (ch.L || ch.R) return 'この会員の下に人がいるため削除できません。下の会員から先に削除してください。';
  if (store.leaders.some(l => l.rootMemberId === memberId)) {
    return 'この会員はリーダーの担当トップになっているため削除できません。先に本部のリーダー管理で対応してください。';
  }
  if (store.conflicts.some(c => c.status === 'pending' && c.memberId === memberId)) {
    return 'この会員には本部の確認待ちの申告があるため削除できません。先に要確認リストで対応してください。';
  }
  return null;
}
// リーダーによる削除。同意・担当範囲のチェックに加え、担当トップ自身は消せない。
async function deleteMemberByLeader(leader, fields) {
  if (!leader.consentAt) return { result: 'error', message: '同意が完了していません。' };
  const memberId = String(fields.memberId || '').trim();
  const scope = subtreeIds(leader.rootMemberId);
  if (!store.members[memberId] || !scope.has(memberId)) {
    return { result: 'error', message: '削除できる会員が見つかりません。' };
  }
  if (memberId === leader.rootMemberId) {
    return { result: 'error', message: '担当トップの会員は削除できません。' };
  }
  const reason = memberDeleteBlockReason(memberId);
  if (reason) return { result: 'error', message: reason };
  const name = store.members[memberId].name;
  delete store.members[memberId];
  await saveStore();
  return { result: 'deleted', message: `「${name}」を削除しました。` };
}
// 本部による削除。共通ガードのみで判定する。
async function deleteMemberByHq(fields) {
  const memberId = String(fields.memberId || '').trim();
  const reason = memberDeleteBlockReason(memberId);
  if (reason) return { result: 'error', message: reason };
  const name = store.members[memberId].name;
  delete store.members[memberId];
  await saveStore();
  return { result: 'deleted', message: `「${name}」を削除しました。` };
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
.chart-bar{display:flex;align-items:center;gap:8px;margin:8px 0}
.chart-bar .zlabel{font-size:12px;color:#555}
.zoombox{display:inline-flex;align-items:center;gap:2px;border:1px solid #bbb;border-radius:8px;padding:2px;background:#fff}
.zoombox button{border:0;background:transparent;min-width:34px;height:30px;border-radius:6px;font-size:16px;font-weight:700;cursor:pointer;color:#14532d;padding:0 6px}
.zoombox button:hover{background:#dcfce7}
.zoombox a.zbtn{display:inline-flex;align-items:center;justify-content:center;min-width:34px;height:30px;border-radius:6px;font-size:16px;font-weight:700;color:#14532d;text-decoration:none;padding:0 6px;box-sizing:border-box}
.zoombox a.zbtn:hover{background:#dcfce7}
.zoombox .pct{min-width:48px;text-align:center;font-size:12.5px;color:#555;font-variant-numeric:tabular-nums}
.chart-wrap{overflow:auto;border:1px solid #e5e7eb;border-radius:8px;background:#fcfdfc;max-height:72vh}
.chart-box{display:inline-block;vertical-align:top}
.chart-scale{display:inline-block;vertical-align:top;transform-origin:0 0}
.chart-level{display:flex;align-items:stretch}
.chart-lvl{flex:0 0 50px;position:sticky;left:0;z-index:3;background:#14532d;color:#fff;font-weight:700;font-size:12.5px;display:flex;align-items:center;justify-content:center}
.chart-lvl-sp{background:#fcfdfc}
.chart-row{display:grid;align-items:start}
.chart-cell{display:flex;flex-direction:column;align-items:center;padding:2px 4px;box-sizing:border-box}
.chart-conn{display:grid;height:20px}
.cn{position:relative;height:20px}
.cn i{position:absolute;display:block;background:#86c98f}
.cn .p{left:50%;top:0;width:2px;height:10px;margin-left:-1px}
.cn .h{left:25%;top:9px;width:50%;height:2px}
.cn .l{left:25%;top:10px;width:2px;height:10px;margin-left:-1px}
.cn .r{left:75%;top:10px;width:2px;height:10px;margin-left:-1px}
.chart-node{background:#fff;border:1.5px solid #14532d;border-radius:8px;padding:6px 10px;text-align:center;width:132px;box-sizing:border-box;line-height:1.45}
.chart-node.chart-root{background:#14532d;color:#fff;font-weight:bold}
.chart-slot{display:block;border:1.5px dashed #94a3b8;color:#14532d;background:#f8fafc;border-radius:8px;padding:7px 10px;text-decoration:none;width:132px;text-align:center;box-sizing:border-box;font-size:14px}
.chart-slot:hover{background:#dcfce7;border-color:#14532d}
form.chart-slot{margin:0;text-align:left}
.chart-slot .slot-title{font-size:12.5px;color:#64748b;text-align:center;margin-bottom:2px}
.chart-slot input{padding:4px 6px;font-size:13px;margin:2px 0;border-radius:5px}
.chart-slot button{width:100%;padding:5px 0;font-size:13px;margin-top:2px}
.chart-empty{border:1.5px dashed #dde3ea;color:#b9c2cf;background:#fbfcfd;border-radius:8px;padding:7px 10px;width:132px;text-align:center;box-sizing:border-box;font-size:13px}
a.chart-empty{text-decoration:none;cursor:pointer}
a.chart-empty:hover{border-color:#94a3b8;color:#64748b;background:#f1f5f9}
.chart-cell:target{scroll-margin-top:16px}
.chart-cell:target .chart-slot{outline:3px solid #f59e0b;outline-offset:2px;background:#fef3c7;box-shadow:0 0 0 4px rgba(245,158,11,.25)}
.search-form{display:flex;gap:8px;align-items:flex-start}
.search-form input{flex:1;width:auto}
.search-result{display:block;border:1px solid #bbf7d0;background:#f0fdf4;border-radius:8px;padding:9px 11px;margin:6px 0;color:#14532d;text-decoration:none}
.search-result:hover{background:#dcfce7}
.chart-note{margin-top:4px}
.chart-drill{display:inline-block;margin-top:5px;background:#fff;color:#14532d;border:1.5px solid #14532d;border-radius:6px;padding:4px 10px;font-size:12.5px;font-weight:700;text-decoration:none}
.chart-drill:hover{background:#dcfce7}
.chart-edit{display:inline-block;margin-top:4px;color:#14532d;font-size:12px;font-weight:600}
.chart-edit:hover{color:#166534;text-decoration-thickness:2px}
.chart-delete{display:inline-block;margin-top:4px;margin-left:10px;color:#b91c1c;font-size:12px;font-weight:600}
.chart-delete:hover{color:#991b1b;text-decoration-thickness:2px}
.chart-focus-link{display:inline-block;margin-top:4px;color:#1d4ed8;font-size:12px;font-weight:600}
.chart-focus-link:hover{color:#1e40af;text-decoration-thickness:2px}
.crumbs{font-size:13.5px;margin:8px 0}.crumbs a{color:#14532d;font-weight:600}
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
// ---------- 組織図チャート描画（リーダー画面用） ----------
// 単独版の組織図（space-2）と同じ考え方で、段（レベル）ごとの行に分けて1枚の図として描く。
// 各行は「その段の列数＝2^段」の等分グリッドで、会員は自分の二分位置（pos）の列に
// 箱として置かれ、親子の間はコネクタ行の線でつながる。左端には「○段目」ラベルが付く。
// 空き枠は親がいる場所だけ、その場で入力できる小さなフォーム（名前＋登録日＋
// 隠しフィールドの親・位置・表示段数）にし、「登録」を押すとその場所へ直接登録される
// （単独版 space-2 のように箱の中で入力が完結する形）。親がいない場所は空白のままで
// 箱は出さない。互換のため ?parentId=<id>&side=<L|R>#add-form のプリセット導線も残す。
// 「表示する段数」（?levels=、2〜10）をデータの段数より大きくすると、データより
// 先の段は全位置を薄い「空き」枠で埋めて、space-2のように先の段まで1枚の図として
// 見せる。段数はデータが必要な段数より少なくはならない（データが優先）。
const CHART_MAX_DEPTH = 10;
const CHART_COL_W = 140;
function collectChartItems(rootId) {
  const map = childrenMap();
  const byDepth = new Map(); // depth -> [{type:'member'|'slot', pos, m?, parentId?, side?}]
  const omitted = new Set(); // 上限段で子がいる会員（省略注記を付ける）
  const cutoff = new Set(); // 上限段にいる会員全員（子がいてもいなくても「＋ 下を増やす」の対象）
  const push = (depth, item) => {
    if (!byDepth.has(depth)) byDepth.set(depth, []);
    byDepth.get(depth).push(item);
  };
  (function walk(id, depth, pos) {
    const m = store.members[id];
    if (!m) return;
    push(depth, { type: 'member', pos, m });
    const ch = map.get(id) || { L: null, R: null };
    if (depth >= CHART_MAX_DEPTH) {
      cutoff.add(id);
      if (ch.L || ch.R) omitted.add(id);
      return;
    }
    if (ch.L) walk(ch.L.memberId, depth + 1, pos * 2);
    else push(depth + 1, { type: 'slot', pos: pos * 2, parentId: id, side: 'L' });
    if (ch.R) walk(ch.R.memberId, depth + 1, pos * 2 + 1);
    else push(depth + 1, { type: 'slot', pos: pos * 2 + 1, parentId: id, side: 'R' });
  })(rootId, 0, 0);
  return { byDepth, omitted, cutoff };
}
function chartNodeHtml(m, editHref, focusHref, deleteHref) {
  if (m.root) return `<div class="chart-node chart-root">${esc(m.name)}</div>`;
  const c = countsFor(m.memberId);
  const editLink = editHref ? `<br><a class="chart-edit" href="${editHref}">名前を修正</a>` : '';
  const deleteLink = deleteHref ? `<a class="chart-delete" href="${deleteHref}">削除</a>` : '';
  const focusLink = focusHref ? `<br><a class="chart-focus-link" href="${focusHref}">この人から下を見る</a>` : '';
  return `<div class="chart-node"><strong>${esc(m.name)}</strong><br><span class="small">ID:${esc(m.memberId)}<br>登録:${esc(m.regDate || '-')}<br>左${c.left}・右${c.right}</span>${editLink}${deleteLink}${focusLink}</div>`;
}
// 空き枠セルのアンカーID。HTML id として安全な文字だけを使い、それ以外は '-' に置き換える。
function slotAnchorId(parentId, side) {
  return 'slot-' + String(parentId).replace(/[^A-Za-z0-9_-]/g, '-') + '-' + side;
}
// 空き枠の中に置く、その場で登録できる小さなフォーム。
// 登録後の戻り先で表示段数を維持するため、現在の表示段数（levels）も隠しフィールドで送る。
// 枝フォーカス表示のときは focus も隠しフィールドで送り、登録後も同じ枝に戻れるようにする
// （フォーカス表示でないときは focus フィールド自体を出さない＝従来のマークアップのまま）。
function chartSlotForm(basePath, slot, today, levels, focusId) {
  const sideLabel = slot.side === 'L' ? '左' : '右';
  return `<form class="chart-slot" method="post" action="${basePath}/add"><div class="slot-title">＋ ${sideLabel}に追加</div>`
    + `<input type="text" name="name" placeholder="名前" maxlength="100">`
    + `<input type="date" name="regDate" value="${today}">`
    + `<input type="hidden" name="parentId" value="${esc(slot.parentId)}">`
    + `<input type="hidden" name="side" value="${slot.side}">`
    + `<input type="hidden" name="levels" value="${levels}">`
    + (focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : '')
    + `<button type="submit">登録</button></form>`;
}
// focusCtx: null（担当トップを先頭に表示）か {focusId, levelOffset}（枝フォーカス表示）。
// 枝フォーカス表示では、指定会員を図の先頭にして、そこから先を新たに CHART_MAX_DEPTH 段
// まで描く。上限段の会員には「＋ 下を増やす」リンク（?focus=<会員ID>）を付け、その会員を
// 先頭に開き直せるようにすることで、深い枝を何度でもたどれる（単独版 space-2 の
// 「10段目以降は枝ごとに伸ばす」ハイブリッド方式と同じ考え方）。段ラベルは
// levelOffset を加えて、担当範囲全体での実際の段数で表示する。
// opts.viewOnly（本部のツリー表示用）: 見る専用モード。空き枠はフォームにせず
// 薄い「空き」枠のままにし、会員の箱に修正リンクを付けない。その代わり、会社以外の
// 全会員の箱に、その会員を先頭に開く「この人から下を見る」リンクを付ける。
// フォーカス（下を増やす）リンクは basePath に対して付くので、本部用は /hq/tree への
// ドリルダウンになる。
function renderChartGrid(rootId, basePath, reqLevels, focusCtx, opts) {
  const today = todayJst();
  const viewOnly = !!(opts && opts.viewOnly);
  // リーダー画面から渡される担当トップの会員ID。その会員の箱には削除リンクを付けない。
  const leaderRootId = opts && opts.leaderRootId ? opts.leaderRootId : null;
  const { byDepth, omitted, cutoff } = collectChartItems(rootId);
  const focusId = focusCtx ? focusCtx.focusId : null;
  const levelOffset = focusCtx ? focusCtx.levelOffset : 0;
  const depths = [...byDepth.keys()].sort((a, b) => a - b);
  const floorDepth = depths.length ? depths[depths.length - 1] : 0;
  const floorLevels = floorDepth + 1;
  // 表示段数：指定があってもデータが必要な段数（従来の表示）より少なくはしない
  let displayLevels = floorLevels;
  if (reqLevels != null) displayLevels = Math.max(floorLevels, reqLevels);
  displayLevels = Math.max(2, displayLevels);
  const maxDepth = displayLevels - 1;
  const cols = Math.pow(2, maxDepth);
  const gridStyle = `grid-template-columns:repeat(${cols},${CHART_COL_W}px)`;
  const cellStyle = (depth, pos) => {
    const span = Math.pow(2, maxDepth - depth);
    return `grid-column:${pos * span + 1} / span ${span}`;
  };
  const connCell = (depth, pos) => `<div class="cn" style="${cellStyle(depth, pos)}"><i class="p"></i><i class="h"></i><i class="l"></i><i class="r"></i></div>`;
  // データのある最下段（floorDepth）の位置→アイテム対応表。データより先の段の「空き」枠は、
  // 自分の祖先にあたるこの段の位置を見て、登録できる空き枠（slot）ならその枠へのリンクにする。
  const floorByPos = new Map((byDepth.get(floorDepth) || []).map(it => [it.pos, it]));
  let html = '';
  for (let d = 0; d <= maxDepth; d++) {
    const items = (byDepth.get(d) || []).slice().sort((a, b) => a.pos - b.pos);
    let cells = '';
    if (d > floorDepth) {
      // データより先の段は、全位置を薄い「空き」枠で埋めて1枚の図として見せる（追加ボタンにはしない）。
      // 祖先が登録できる空き枠の場合は、その枠へ移動できるリンクにする（枠自体では登録しない）。
      for (let pos = 0; pos < Math.pow(2, d); pos++) {
        const ancestor = floorByPos.get(pos >> (d - floorDepth));
        const inner = !viewOnly && ancestor && ancestor.type === 'slot'
          ? `<a class="chart-empty" href="#${slotAnchorId(ancestor.parentId, ancestor.side)}">空き</a>`
          : `<div class="chart-empty">空き</div>`;
        cells += `<div class="chart-cell" style="${cellStyle(d, pos)}">${inner}</div>`;
      }
    } else {
      for (const it of items) {
        if (it.type === 'member') {
          const note = omitted.has(it.m.memberId) ? `<div class="small chart-note">（これより下は省略しています）</div>` : '';
          // 会員の箱から、その会員を対象にした修正フォームへ移動できるようにする。
          // いま見ている枝（focus）と、明示指定された表示段数（levels）があれば引き継ぐ。
          // 見る専用モード（本部）では修正リンクを付けない。
          let editHref = '';
          if (!viewOnly) {
            editHref = `${basePath}?edit=${encodeURIComponent(it.m.memberId)}`;
            if (focusId) editHref += `&focus=${encodeURIComponent(focusId)}`;
            if (reqLevels != null) editHref += `&levels=${reqLevels}`;
            editHref += '#edit-form';
          }
          // リーダー画面では、担当トップ以外の会員の箱に「削除」リンクも付ける
          // （削除の確認カードへ移動するだけ。実際に消せるかはサーバー側の共通ガードで判定）。
          let deleteHref = '';
          if (!viewOnly && it.m.memberId !== leaderRootId) {
            deleteHref = `${basePath}?delete=${encodeURIComponent(it.m.memberId)}`;
            if (focusId) deleteHref += `&focus=${encodeURIComponent(focusId)}`;
            if (reqLevels != null) deleteHref += `&levels=${reqLevels}`;
            deleteHref += '#delete-form';
          }
          // 上限段の会員は、子がいる・いないに関わらず、その会員を先頭に開き直す
          // 「＋ 下を増やす」を出す（空き枠も上限より下にあるため）。明示指定された
          // 表示段数がある場合はリンクにも引き継ぐ。
          const drill = cutoff.has(it.m.memberId)
            ? `<a class="chart-drill" href="${basePath}?focus=${encodeURIComponent(it.m.memberId)}${reqLevels != null ? '&levels=' + reqLevels : ''}#chartWrap">＋ 下を増やす</a>`
            : '';
          // 見る専用モード（本部）では、会社以外の全会員の箱から、その会員を先頭にした
          // 図へ直接移動できるリンクを付ける（リーダー画面には付けない）。
          const focusHref = viewOnly && !it.m.root
            ? `${basePath}?focus=${encodeURIComponent(it.m.memberId)}#chartWrap`
            : '';
          cells += `<div class="chart-cell" style="${cellStyle(d, it.pos)}">${chartNodeHtml(it.m, editHref, focusHref, deleteHref)}${note}${drill}</div>`;
        } else if (viewOnly) {
          // 見る専用モード（本部）では、親がいる空き場所も入力フォームにせず「空き」枠で見せる
          cells += `<div class="chart-cell" style="${cellStyle(d, it.pos)}"><div class="chart-empty">空き</div></div>`;
        } else {
          cells += `<div class="chart-cell" id="${slotAnchorId(it.parentId, it.side)}" style="${cellStyle(d, it.pos)}">${chartSlotForm(basePath, it, today, displayLevels, focusId)}</div>`;
        }
      }
    }
    html += `<div class="chart-level"><div class="chart-lvl">${d + 1 + levelOffset}段目</div><div class="chart-row" style="${gridStyle}">${cells}</div></div>`;
    if (d < maxDepth) {
      let conns = '';
      if (maxDepth > floorDepth && d >= floorDepth) {
        // データより先の段へつながる行では、その段の全ての箱から線を引いて図を1つにつなぐ
        if (d > floorDepth) {
          for (let pos = 0; pos < Math.pow(2, d); pos++) conns += connCell(d, pos);
        } else {
          for (const it of items) conns += connCell(d, it.pos);
        }
      } else {
        for (const it of items) {
          if (it.type !== 'member') continue;
          conns += connCell(d, it.pos);
        }
      }
      html += `<div class="chart-level"><div class="chart-lvl chart-lvl-sp"></div><div class="chart-conn" style="${gridStyle}">${conns}</div></div>`;
    }
  }
  return { html, levels: displayLevels };
}
function addMemberForm(action, scopeIds, preset) {
  const pre = preset || {};
  return `<form method="post" action="${action}" id="add-form">
  <label>名前<input name="name" required maxlength="100"></label>
  <label>登録日<input name="regDate" required type="date"></label>
  <label>上に付く人（親）<select name="parentId">${memberOptions(pre.parentId || '', scopeIds)}</select></label>
  <label>位置<select name="side"><option value="L" ${pre.side === 'L' ? 'selected' : ''}>左</option><option value="R" ${pre.side === 'R' ? 'selected' : ''}>右</option></select></label>
  <button type="submit">登録する</button></form>`;
}

// ---------- 本部 ----------
function hqNav() {
  return `<p class="nav"><a href="/hq">組織図（全体）</a><a href="/hq/tree">組織図（ツリー）</a><a href="/hq/members">会員一覧</a><a href="/hq/leaders">リーダー管理</a><a href="/hq/conflicts">要確認リスト</a><a href="/hq/export.csv">CSV書き出し</a></p>
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
// 本部の組織図（ツリー表示）。リーダー画面と同じ段グリッドの組織図を、会社を先頭に
// 全体表示する見る専用ページ。空き枠は入力フォームにせず「空き」枠のまま見せ、
// 会員の追加・修正は従来の画面（会員一覧・リーダー画面）で行う。
// 深い組織のために、リーダー画面と同じ枝フォーカス（?focus=）が使える。
async function handleHqTree(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  const basePath = '/hq/tree';
  // 表示する段数（?levels=）。2〜10に丸め、データが必要な段数より少なくはならない（リーダー画面と同じ）。
  let reqLevels = null;
  const lvRaw = url.searchParams.get('levels');
  if (lvRaw !== null && /^\d+$/.test(lvRaw.trim())) {
    reqLevels = Math.min(CHART_MAX_DEPTH, Math.max(2, parseInt(lvRaw, 10)));
  }
  // 枝フォーカス（?focus=<会員ID>）: 実在する会員だけを有効とし、それ以外は全体表示に落とす。
  // 会社自身の指定は全体表示と同じ。
  let focusId = null;
  const fParam = (url.searchParams.get('focus') || '').trim();
  if (fParam && fParam !== ROOT_ID && store.members[fParam]) focusId = fParam;
  let focusPath = null;
  let levelOffset = 0;
  if (focusId) {
    focusPath = [];
    let cur = store.members[focusId];
    while (cur) {
      focusPath.unshift(cur.memberId);
      if (cur.memberId === ROOT_ID) break;
      cur = cur.parentId ? store.members[cur.parentId] : null;
    }
    if (focusPath[0] !== ROOT_ID) { focusId = null; focusPath = null; }
    else levelOffset = focusPath.length - 1;
  }
  const chartTopId = focusId || ROOT_ID;
  const chart = renderChartGrid(chartTopId, basePath, reqLevels, focusId ? { focusId, levelOffset } : null, { viewOnly: true });
  const lvMinus = Math.min(CHART_MAX_DEPTH, Math.max(2, chart.levels - 1));
  const lvPlus = Math.min(CHART_MAX_DEPTH, Math.max(2, chart.levels + 1));
  // 段数の－/＋リンクは、枝フォーカス表示ではフォーカスを引き継ぐ
  const lvHref = (n) => focusId ? `${basePath}?focus=${encodeURIComponent(focusId)}&levels=${n}` : `${basePath}?levels=${n}`;
  let focusBanner = '';
  if (focusId) {
    const fm = store.members[focusId];
    const crumbs = focusPath.map((id, i) => {
      const mm = store.members[id];
      if (i === focusPath.length - 1) return `<strong>${esc(mm.name)}</strong>`;
      if (i === 0) return `<a href="${basePath}">${esc(mm.name)}</a>`;
      return `<a href="${basePath}?focus=${encodeURIComponent(id)}">${esc(mm.name)}</a>`;
    }).join(' ＞ ');
    focusBanner = `<p class="crumbs">表示中の枝：${crumbs}</p>
  <p class="small">いまは <strong>${esc(fm.name)}</strong> さん（全体では${levelOffset + 1}段目）の下の図を表示しています。先の段の会員の「＋ 下を増やす」を押せば、その枝をさらに下へたどれます。<a href="${basePath}">全体の図に戻る</a></p>`;
  }
  const body = `${hqNav()}
  <div class="card"><h2>組織図（ツリー表示・会社全体）</h2>
  <p class="small">会社全体の組織図です。この画面は見る専用です（空き枠は入力できません）。会員の追加は「会員一覧」から、登録内容の修正はリーダー画面など従来の画面から行ってください。</p>
  <p class="small">「表示する段数」の＋を押すと、まだ誰もいない先の段まで図を広げて見られます。各会員の「この人から下を見る」を押すと、その人を先頭にした図へ移動できます。図の一番下の段にいる会員の「＋ 下を増やす」を押すと、その人を先頭にして、さらに下の枝を見られます（何度でも繰り返せます）。</p>
  ${focusBanner}
  <div class="chart-bar"><span class="zlabel">表示する段数：</span><span class="zoombox"><a class="zbtn" href="${lvHref(lvMinus)}" aria-label="段数を減らす">−</a><span class="pct">${chart.levels}段</span><a class="zbtn" href="${lvHref(lvPlus)}" aria-label="段数を増やす">＋</a></span><span class="zlabel">図の大きさ：</span><span class="zoombox"><button type="button" id="zOut" aria-label="縮小">−</button><span class="pct" id="zPct">100%</span><button type="button" id="zIn" aria-label="拡大">＋</button><button type="button" id="zFit">等倍</button></span><span class="zlabel">横にスクロールできます</span></div>
  <div class="chart-wrap" id="chartWrap"><div class="chart-box" id="chartBox"><div class="chart-scale" id="chartScale">${chart.html}</div></div></div></div>
  <script>(function(){var z=1;var scale=document.getElementById('chartScale');var box=document.getElementById('chartBox');var pct=document.getElementById('zPct');if(!scale||!box||!pct)return;function apply(){scale.style.transform='scale('+z+')';box.style.width=Math.ceil(scale.scrollWidth*z)+'px';box.style.height=Math.ceil(scale.scrollHeight*z)+'px';pct.textContent=Math.round(z*100)+'%';}document.getElementById('zIn').addEventListener('click',function(){z=Math.min(2,Math.round(z*1.25*100)/100);apply();});document.getElementById('zOut').addEventListener('click',function(){z=Math.max(0.1,Math.round(z/1.25*100)/100);apply();});document.getElementById('zFit').addEventListener('click',function(){z=1;apply();});apply();})();</script>`;
  return send(res, 200, page('本部：組織図（ツリー）', body, flashFrom(url)));
}
async function handleHqMembers(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  if (req.method === 'POST' && url.pathname === '/hq/members/delete') {
    const body0 = await readBody(req);
    const r = await deleteMemberByHq(Object.fromEntries(body0));
    return redirect(res, back('/hq/members', r.result === 'error' ? 'err' : 'msg', r.message));
  }
  if (req.method === 'POST') {
    const body0 = await readBody(req);
    const r = await addMember({ type: 'hq' }, Object.fromEntries(body0));
    return redirect(res, back('/hq/members', r.result === 'error' ? 'err' : 'msg', r.message));
  }
  const q = (url.searchParams.get('q') || '').trim().toLowerCase();
  let list = Object.values(store.members);
  if (q) list = list.filter(m => m.name.toLowerCase().includes(q) || m.memberId.toLowerCase().includes(q));
  list.sort((a, b) => (a.root ? 0 : 1) - (b.root ? 0 : 1) || a.memberId.localeCompare(b.memberId));
  const memberCount = list.filter(m => !m.root).length;
  const rows = list.map(m => {
    const c = countsFor(m.memberId);
    const parent = m.parentId ? store.members[m.parentId] : null;
    const treeHref = m.root ? '/hq/tree' : `/hq/tree?focus=${encodeURIComponent(m.memberId)}`;
    const deleteCell = m.root ? '<td></td>' : `<td><a href="/hq/members?delete=${encodeURIComponent(m.memberId)}#delete-form">削除</a></td>`;
    return `<tr><td>${esc(m.memberId)}</td><td>${esc(m.name)}</td><td>${esc(m.regDate || '')}</td><td>${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')}</td><td>${m.side === 'L' ? '左' : m.side === 'R' ? '右' : ''}</td><td>${c.left}</td><td>${c.right}</td><td>${esc(m.enteredBy || '')}</td><td><a href="${treeHref}">ツリーで見る</a></td>${deleteCell}</tr>`;
  }).join('');
  // 会員の削除（?delete=<会員ID>）。実在する会員（会社の枠以外）のときだけ確認カードを出す。
  // 下に人がいる等で消せない場合は理由を表示し、削除ボタン自体を出さない
  // （実際の可否は POST 時に共通ガード memberDeleteBlockReason でも判定する）。
  const hqDeleteParam = (url.searchParams.get('delete') || '').trim();
  const hqDeleteMember = hqDeleteParam && hqDeleteParam !== ROOT_ID && store.members[hqDeleteParam] ? store.members[hqDeleteParam] : null;
  let hqDeleteCard = '';
  if (hqDeleteMember) {
    const parent = hqDeleteMember.parentId ? store.members[hqDeleteMember.parentId] : null;
    const blockReason = memberDeleteBlockReason(hqDeleteMember.memberId);
    const actionHtml = blockReason
      ? `<p class="flash ng">${esc(blockReason)}</p>`
      : `<p class="small">この操作は元に戻せません。間違えて配置した会員だけを削除してください。</p>
  <form method="post" action="/hq/members/delete">
  <input type="hidden" name="memberId" value="${esc(hqDeleteMember.memberId)}">
  <button class="danger" type="submit">削除する</button> <a class="btn sub" href="/hq/members">キャンセル</a></form>`;
    hqDeleteCard = `<div class="card" id="delete-form"><h3>会員を削除する</h3>
  <p class="small">対象：${esc(hqDeleteMember.name)}（${esc(hqDeleteMember.memberId)}） / 登録：${esc(hqDeleteMember.regDate || '-')} / 親：${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')} の${hqDeleteMember.side === 'L' ? '左' : hqDeleteMember.side === 'R' ? '右' : ''}</p>
  ${actionHtml}</div>`;
  }
  const body = `${hqNav()}<div class="card"><h2>会員一覧（${memberCount}件）</h2>
  <form method="get" action="/hq/members"><input name="q" value="${esc(q)}" placeholder="名前・会員IDで検索"><button type="submit">検索</button></form>
  <table><tr><th>会員ID</th><th>名前</th><th>登録日</th><th>親</th><th>位置</th><th>左人数</th><th>右人数</th><th>入力者</th><th>ツリー</th><th>削除</th></tr>${rows || '<tr><td colspan="10">該当なし</td></tr>'}</table></div>
  ${hqDeleteCard}
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
function leaderFocusPart(leader, rawFocus) {
  const focus = String(rawFocus || '').trim();
  if (focus && store.members[focus] && subtreeIds(leader.rootMemberId).has(focus)) {
    return `focus=${encodeURIComponent(focus)}&`;
  }
  return '';
}
function leaderLevelsPart(rawLevels) {
  const raw = String(rawLevels || '').trim();
  if (/^\d+$/.test(raw)) {
    const levels = parseInt(raw, 10);
    if (levels >= 2 && levels <= CHART_MAX_DEPTH) return `levels=${levels}&`;
  }
  return '';
}
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
    if (req.method === 'POST' && (url.pathname.endsWith('/add') || url.pathname.endsWith('/edit') || url.pathname.endsWith('/delete'))) {
      return redirect(res, back(basePath, 'err', '同意が完了してから入力してください。'));
    }
    return send(res, 200, page('同意のお願い', `${CONSENT_TEXT}
    <form method="post" action="${basePath}/consent"><button type="submit">同意して始める</button></form>`, flashFrom(url)));
  }
  if (req.method === 'POST' && url.pathname.endsWith('/add')) {
    const body0 = await readBody(req);
    const r = await addMember({ type: 'leader', leader }, Object.fromEntries(body0));
    // 枝フォーカス表示の図内フォームからの登録では、フォーカスも引き継いで同じ枝に戻る。
    // 実在し担当範囲内にある会員のときだけ引き継ぎ、それ以外は無視する（範囲は広げない）。
    const focusPart = leaderFocusPart(leader, body0.get('focus'));
    // 組織図内のフォームからの登録では、見ていた表示段数（levels）を引き継いで戻る。
    // 2〜10の整数のときだけ引き継ぎ、それ以外は無視する（下のフォーム等は従来どおり）。
    const lvPart = leaderLevelsPart(body0.get('levels'));
    const key = r.result === 'error' ? 'err' : 'msg';
    return redirect(res, `${basePath}?${focusPart}${lvPart}${key}=${encodeURIComponent(r.message)}`);
  }
  if (req.method === 'POST' && url.pathname.endsWith('/edit')) {
    const body0 = await readBody(req);
    const r = await updateMemberByLeader(leader, Object.fromEntries(body0));
    const focusPart = leaderFocusPart(leader, body0.get('focus'));
    const lvPart = leaderLevelsPart(body0.get('levels'));
    const key = r.result === 'error' ? 'err' : 'msg';
    return redirect(res, `${basePath}?${focusPart}${lvPart}${key}=${encodeURIComponent(r.message)}`);
  }
  if (req.method === 'POST' && url.pathname.endsWith('/delete')) {
    const body0 = await readBody(req);
    const r = await deleteMemberByLeader(leader, Object.fromEntries(body0));
    // 削除した会員自身がフォーカス先だった場合は leaderFocusPart が引き継がない
    // （実在しない会員は無効）ため、その場合は担当トップ表示に戻る。
    const focusPart = leaderFocusPart(leader, body0.get('focus'));
    const lvPart = leaderLevelsPart(body0.get('levels'));
    const key = r.result === 'error' ? 'err' : 'msg';
    return redirect(res, `${basePath}?${focusPart}${lvPart}${key}=${encodeURIComponent(r.message)}`);
  }
  const scope = subtreeIds(leader.rootMemberId);
  const root = store.members[leader.rootMemberId];
  // 会員検索（?q=）。フォーカス表示の有無に関わらず、担当範囲（担当トップ以下）全体から
  // 名前の部分一致（大文字小文字は区別しない）と会員IDの部分一致で探す。範囲外は出さない。
  const searchQuery = (url.searchParams.get('q') || '').trim();
  let searchResultsHtml = '';
  if (searchQuery) {
    const needle = searchQuery.toLowerCase();
    const depthMap = scopeDepthMap(leader.rootMemberId);
    const matches = [];
    for (const id of depthMap.keys()) {
      const m = store.members[id];
      if (!m) continue;
      if (m.name.toLowerCase().includes(needle) || m.memberId.toLowerCase().includes(needle)) matches.push(m);
    }
    if (!matches.length) {
      searchResultsHtml = '<p>見つかりませんでした</p>';
    } else {
      const shown = matches.slice(0, 50);
      const items = shown.map(m => {
        const depth = (depthMap.get(m.memberId) || 0) + 1;
        const parent = m.memberId === leader.rootMemberId
          ? '担当トップ'
          : (m.parentId && store.members[m.parentId] ? store.members[m.parentId].name : '');
        const href = m.memberId === leader.rootMemberId
          ? basePath
          : `${basePath}?focus=${encodeURIComponent(m.memberId)}#chartWrap`;
        return `<a class="search-result" href="${href}"><strong>${esc(m.name)}</strong> <span class="small">ID:${esc(m.memberId)} / 登録:${esc(m.regDate || '-')} / ${depth}段目 / 親：${esc(parent)}</span></a>`;
      }).join('');
      const note = matches.length > shown.length
        ? `<p class="small">${matches.length}件中、最初の${shown.length}件まで表示しています。</p>`
        : `<p class="small">${matches.length}件見つかりました。</p>`;
      searchResultsHtml = `${note}${items}`;
    }
  }
  const searchCard = `<div class="card"><h3>会員を検索</h3>
  <form class="search-form" method="get" action="${basePath}"><input name="q" value="${esc(searchQuery)}" placeholder="名前・会員IDで検索"><button type="submit">検索</button></form>${searchResultsHtml}</div>`;
  // 組織図の空き枠から来たプリセット（?parentId=&side=）。担当範囲内の親だけを受け付ける。
  let preset = null;
  const pParam = url.searchParams.get('parentId');
  const sParam = url.searchParams.get('side');
  if (pParam && scope.has(pParam) && (sParam === 'L' || sParam === 'R')) {
    preset = { parentId: pParam, side: sParam };
  }
  const presetParent = preset ? store.members[preset.parentId] : null;
  const presetNote = preset
    ? `<p class="flash ok">組織図で選択中：${esc(presetParent ? presetParent.name + '（' + presetParent.memberId + '）' : preset.parentId)} の${preset.side === 'L' ? '左' : '右'}（下のフォームに反映済みです。そのまま入力するか、プルダウンで変更もできます）</p>`
    : '';
  // 表示する段数（?levels=）。2〜10に丸め、データが必要な段数より少なくはならない（renderChartGrid側で下限を確保）。
  let reqLevels = null;
  const lvRaw = url.searchParams.get('levels');
  if (lvRaw !== null && /^\d+$/.test(lvRaw.trim())) {
    reqLevels = Math.min(CHART_MAX_DEPTH, Math.max(2, parseInt(lvRaw, 10)));
  }
  // 枝フォーカス（?focus=<会員ID>）: その会員を図の先頭にして、その枝だけを深く見る表示。
  // 実在し担当範囲内にある会員だけを有効とし、それ以外（範囲外・存在しないID）は
  // 担当トップ表示に落とす（範囲外の情報は一切出さない）。担当トップ自身の指定は通常表示と同じ。
  let focusId = null;
  const fParam = (url.searchParams.get('focus') || '').trim();
  if (fParam && fParam !== leader.rootMemberId && store.members[fParam] && scope.has(fParam)) focusId = fParam;
  const chartTopId = focusId || leader.rootMemberId;
  // 担当トップからフォーカス会員までの経路（パンくず用）と、全体での段数オフセットを求める
  let focusPath = null;
  let levelOffset = 0;
  if (focusId) {
    focusPath = [];
    let cur = store.members[focusId];
    while (cur) {
      focusPath.unshift(cur.memberId);
      if (cur.memberId === leader.rootMemberId) break;
      cur = cur.parentId ? store.members[cur.parentId] : null;
    }
    if (focusPath[0] !== leader.rootMemberId) { focusId = null; focusPath = null; }
    else levelOffset = focusPath.length - 1;
  }
  const chart = renderChartGrid(chartTopId, basePath, reqLevels, focusId ? { focusId, levelOffset } : null, { leaderRootId: leader.rootMemberId });
  const lvMinus = Math.min(CHART_MAX_DEPTH, Math.max(2, chart.levels - 1));
  const lvPlus = Math.min(CHART_MAX_DEPTH, Math.max(2, chart.levels + 1));
  // 段数の－/＋リンクは、枝フォーカス表示ではフォーカスを引き継ぐ
  const lvHref = (n) => focusId ? `${basePath}?focus=${encodeURIComponent(focusId)}&levels=${n}` : `${basePath}?levels=${n}`;
  let focusBanner = '';
  if (focusId) {
    const fm = store.members[focusId];
    const crumbs = focusPath.map((id, i) => {
      const mm = store.members[id];
      if (i === focusPath.length - 1) return `<strong>${esc(mm.name)}</strong>`;
      if (i === 0) return `<a href="${basePath}">${esc(mm.name)}</a>`;
      return `<a href="${basePath}?focus=${encodeURIComponent(id)}">${esc(mm.name)}</a>`;
    }).join(' ＞ ');
    focusBanner = `<p class="crumbs">表示中の枝：${crumbs}</p>
  <p class="small">いまは <strong>${esc(fm.name)}</strong> さん（担当範囲全体では${levelOffset + 1}段目）の下の図を表示しています。先の段の会員の「＋ 下を増やす」を押せば、その枝をさらに下へたどれます。<a href="${basePath}">担当トップの図に戻る</a>（下のフォームから追加できる範囲は、担当範囲全体のままで変わりません）</p>`;
  }
  // 会員情報の修正（?edit=<会員ID>）。実在し担当範囲内にある会員だけを対象にし、
  // それ以外では修正カード自体を出さない。戻り先として現在の focus / levels を引き継ぐ。
  const editParam = (url.searchParams.get('edit') || '').trim();
  const editMember = editParam && store.members[editParam] && scope.has(editParam) ? store.members[editParam] : null;
  let editCard = '';
  if (editMember) {
    editCard = `<div class="card" id="edit-form"><h3>会員情報を修正する</h3>
  <p class="small">対象：${esc(editMember.name)}（${esc(editMember.memberId)}）</p>
  <form method="post" action="${basePath}/edit">
  <label>名前<input name="name" value="${esc(editMember.name)}" required maxlength="100"></label>
  <label>登録日<input name="regDate" type="date" value="${esc(editMember.regDate || '')}"></label>
  <input type="hidden" name="memberId" value="${esc(editMember.memberId)}">
  ${focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : ''}
  ${reqLevels != null ? `<input type="hidden" name="levels" value="${reqLevels}">` : ''}
  <button type="submit">変更を保存</button> <a class="btn sub" href="${basePath}">キャンセル</a></form></div>`;
  }
  // 会員の削除（?delete=<会員ID>）。実在し担当範囲内にあり、担当トップでない会員だけを
  // 対象に確認カードを出す。下に人がいる等で消せない場合は理由を表示し、削除ボタン
  // 自体を出さない（実際の可否は POST 時に共通ガードでも判定する）。
  const deleteParam = (url.searchParams.get('delete') || '').trim();
  const deleteTarget = deleteParam && deleteParam !== leader.rootMemberId && store.members[deleteParam] && scope.has(deleteParam)
    ? store.members[deleteParam] : null;
  let deleteCard = '';
  if (deleteTarget) {
    const parent = deleteTarget.parentId ? store.members[deleteTarget.parentId] : null;
    const blockReason = memberDeleteBlockReason(deleteTarget.memberId);
    const actionHtml = blockReason
      ? `<p class="flash ng">${esc(blockReason)}</p>`
      : `<p class="small">この操作は元に戻せません。間違えて配置した会員だけを削除してください。</p>
  <form method="post" action="${basePath}/delete">
  <input type="hidden" name="memberId" value="${esc(deleteTarget.memberId)}">
  ${focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : ''}
  ${reqLevels != null ? `<input type="hidden" name="levels" value="${reqLevels}">` : ''}
  <button class="danger" type="submit">削除する</button> <a class="btn sub" href="${basePath}">キャンセル</a></form>`;
    deleteCard = `<div class="card" id="delete-form"><h3>会員を削除する</h3>
  <p class="small">対象：${esc(deleteTarget.name)}（${esc(deleteTarget.memberId)}） / 登録：${esc(deleteTarget.regDate || '-')} / 親：${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')} の${deleteTarget.side === 'L' ? '左' : deleteTarget.side === 'R' ? '右' : ''}</p>
  ${actionHtml}</div>`;
  }
  const body = `${searchCard}<div class="card"><h2>${esc(leader.name)} さんの組織</h2>
  <p class="small">担当トップ：${esc(root ? root.name + '（' + root.memberId + '）' : leader.rootMemberId)} / ここから下だけが表示・入力できます。</p>
  <p class="small">空いている場所の枠に名前を入力して「登録」を押すと、その場所にそのまま登録できます（登録日には今日の日付が入っています。変える場合は日付を変更してください）。下のフォームから親と位置を選んで登録することもできます。</p>
  <p class="small">「表示する段数」の＋を押すと、まだ誰もいない先の段まで図を広げて見られます。先の段の「空き」を押すと、その枝の登録できる空き枠へ移動します。</p>
  <p class="small">図の一番下の段にいる会員の「＋ 下を増やす」を押すと、その人を先頭にして、さらに下の枝を見たり入力したりできます（何度でも繰り返せます）。</p>
  ${focusBanner}
  <div class="chart-bar"><span class="zlabel">表示する段数：</span><span class="zoombox"><a class="zbtn" href="${lvHref(lvMinus)}" aria-label="段数を減らす">−</a><span class="pct">${chart.levels}段</span><a class="zbtn" href="${lvHref(lvPlus)}" aria-label="段数を増やす">＋</a></span><span class="zlabel">図の大きさ：</span><span class="zoombox"><button type="button" id="zOut" aria-label="縮小">−</button><span class="pct" id="zPct">100%</span><button type="button" id="zIn" aria-label="拡大">＋</button><button type="button" id="zFit">等倍</button></span><span class="zlabel">横にスクロールできます</span></div>
  <div class="chart-wrap" id="chartWrap"><div class="chart-box" id="chartBox"><div class="chart-scale" id="chartScale">${chart.html}</div></div></div></div>
  ${editCard}
  ${deleteCard}
  <div class="card"><h3>会員を追加する</h3>${presetNote}${addMemberForm(basePath + '/add', scope, preset)}</div>
  <script>(function(){var z=1;var scale=document.getElementById('chartScale');var box=document.getElementById('chartBox');var pct=document.getElementById('zPct');if(!scale||!box||!pct)return;function apply(){scale.style.transform='scale('+z+')';box.style.width=Math.ceil(scale.scrollWidth*z)+'px';box.style.height=Math.ceil(scale.scrollHeight*z)+'px';pct.textContent=Math.round(z*100)+'%';}document.getElementById('zIn').addEventListener('click',function(){z=Math.min(2,Math.round(z*1.25*100)/100);apply();});document.getElementById('zOut').addEventListener('click',function(){z=Math.max(0.1,Math.round(z/1.25*100)/100);apply();});document.getElementById('zFit').addEventListener('click',function(){z=1;apply();});apply();})();</script>`;
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
    if (p === '/hq/tree') return handleHqTree(req, res, url);
    if (p === '/hq/members' || p === '/hq/members/add' || p === '/hq/members/delete') return handleHqMembers(req, res, url);
    if (p === '/hq/leaders') return handleHqLeaders(req, res, url);
    if (p === '/hq/conflicts') return handleHqConflicts(req, res, url);
    if (p === '/hq/export.csv') return handleExportCsv(req, res);
    const lm = p.match(/^\/leader\/([A-Za-z0-9_-]+)(\/(consent|add|edit|delete))?$/);
    if (lm) return handleLeader(req, res, url, lm[1]);
    return send(res, 404, page('見つかりません', '<div class="card"><h2>ページが見つかりません</h2></div>'));
  } catch (e) {
    console.error(e);
    return send(res, 500, page('エラー', '<div class="card"><h2>サーバーエラーが発生しました</h2><p>時間をおいて再度お試しください。</p></div>'));
  }
});

if (require.main === module) {
  boot().then(() => {
    server.listen(PORT, HOST, () => {
      console.log(`会員一括管理システムを起動しました: http://${HOST}:${PORT} (data: ${DB_MODE ? 'Neon Postgres (DATABASE_URL)' : DATA_FILE})`);
      if (!store.settings.hqPassHash) console.log('初回セットアップ: /setup で本部IDとパスワードを設定してください。');
    });
  }).catch(e => {
    // DBに繋がらない等で状態を読めない場合は、空データで起動して本データを壊さないよう終了する
    console.error('起動に失敗しました:', e.message);
    process.exit(1);
  });
} else if (!DB_MODE) {
  loadStore();
}
module.exports = { server, loadStore, boot };
