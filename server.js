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
const zlib = require('zlib');
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
  return a.name === b.name && a.parentId === b.parentId && a.side === b.side && a.regDate === b.regDate
    && (a.introducer || '') === (b.introducer || '');
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
  // 紹介者（必須入力の自由記入。空欄なら登録しない）
  const introducer = String(fields.introducer || '').trim();
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
  if (!introducer) return { result: 'error', message: '紹介者を入力してください。' };
  if (introducer.length > 100) return { result: 'error', message: '紹介者は100文字以内で入力してください。' };
  if (!validDate(regDate)) return { result: 'error', message: '登録日は YYYY-MM-DD 形式の正しい日付で入力してください。' };
  if (side !== 'L' && side !== 'R') return { result: 'error', message: '位置は左か右を選んでください。' };
  const parent = store.members[parentId];
  if (!parent) return { result: 'error', message: '上に付く人（親）の会員IDが見つかりません。' };

  if (actor.type === 'leader') {
    if (!actor.leader.consentAt) return { result: 'error', message: '同意が完了していません。' };
    const scope = subtreeIds(actor.leader.rootMemberId);
    if (!scope.has(parentId)) return { result: 'error', message: '自分の配下の範囲外には追加できません。' };
  }

  const claimed = { memberId, name, parentId, side, regDate, introducer };
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

// ---------- 会員情報の修正（本部） ----------
// 本部が組織図（ツリー）から会員の名前・登録日を直接修正する。権限は本部全体で、
// 会社の枠は名前だけ変更できる（削除はできない）。リーダーの修正と同様、これは訂正であり新規登録の
// 申告ではないため、本部の要確認リスト（conflicts）は作らない。会員ID・親・位置・
// 入力者は変更しない。
async function updateMemberByHq(fields) {
  const memberId = String(fields.memberId || '').trim();
  const member = store.members[memberId];
  if (!member) {
    return { result: 'error', message: '修正できる会員が見つかりません。' };
  }
  // 会社（一番上）の枠は、名前だけ変更できる（本部のみ）。登録日・紹介者・位置の
  // 概念が無い枠なので、それ以外の項目は受け付けず、削除も従来どおりできない。
  if (memberId === ROOT_ID || member.root) {
    const rootName = String(fields.name || '').trim();
    if (!rootName || rootName.length > 100) return { result: 'error', message: '名前を入力してください（100文字まで）。' };
    member.name = rootName;
    await saveStore();
    return { result: 'updated', message: '一番上の名前を変更しました。' };
  }
  const name = String(fields.name || '').trim();
  if (!name || name.length > 100) return { result: 'error', message: '名前を入力してください（100文字まで）。' };
  const submittedDate = String(fields.regDate || '').trim();
  let regDate = member.regDate || '';
  if (submittedDate) {
    if (!validDate(submittedDate)) return { result: 'error', message: '登録日は YYYY-MM-DD 形式の正しい日付で入力してください。' };
    regDate = submittedDate;
  }
  // 紹介者：フォームから送られてきた場合は必須とする（空で送られたらエラーにし、
  // 名前・登録日・紹介者のどれも保存しない）。フィールド自体が無い送信では、今の値を
  // そのまま残す。
  let newIntroducer;
  if (fields.introducer !== undefined) {
    newIntroducer = String(fields.introducer || '').trim();
    if (!newIntroducer) return { result: 'error', message: '紹介者を入力してください。' };
    if (newIntroducer.length > 100) return { result: 'error', message: '紹介者は100文字以内で入力してください。' };
  }
  member.name = name;
  member.regDate = regDate;
  if (newIntroducer !== undefined) member.introducer = newIntroducer;
  await saveStore();
  return { result: 'updated', message: '会員情報を修正しました。' };
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
  // 紹介者：フォームから送られてきた場合は必須とする（空で送られたらエラーにし、
  // 名前・登録日・紹介者のどれも保存しない）。フィールド自体が無い送信では、今の値を
  // そのまま残す。
  let newIntroducer;
  if (fields.introducer !== undefined) {
    newIntroducer = String(fields.introducer || '').trim();
    if (!newIntroducer) return { result: 'error', message: '紹介者を入力してください。' };
    if (newIntroducer.length > 100) return { result: 'error', message: '紹介者は100文字以内で入力してください。' };
  }
  member.name = name;
  member.regDate = regDate;
  if (newIntroducer !== undefined) member.introducer = newIntroducer;
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
// - 有効な（active）リーダーの担当トップになっている（そのリーダーの専用リンクが壊れる）。
//   無効化済み（revoked）のリーダーは締めない: リーダー管理で無効化すれば担当トップを
//   削除できるようにし、削除後にそのリーダーを再発行しようとした場合は再発行側で拒否する。
// - その会員について本部の確認待ち（pending）の申告がある（先に要確認リストで対応する）
function memberDeleteBlockReason(memberId) {
  const member = store.members[memberId];
  if (!member) return '対象の会員が見つかりません。';
  if (memberId === ROOT_ID || member.root) return '会社の枠は削除できません。';
  const ch = childrenMap().get(memberId) || { L: null, R: null };
  if (ch.L || ch.R) return 'この会員の下に人がいるため削除できません。下の会員から先に削除してください。';
  if (store.leaders.some(l => l.status === 'active' && l.rootMemberId === memberId)) {
    return 'この会員はリーダーの担当トップになっているため削除できません。先に本部のリーダー管理で、そのリーダーを「無効化」してください。';
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
.breakout{width:min(1760px,97vw);margin-left:50%;transform:translateX(-50%);box-sizing:border-box}
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
.zoombox .pctin{width:56px;text-align:center;font-size:12.5px;color:#555;font-variant-numeric:tabular-nums;border:0;background:transparent;padding:2px 0;box-sizing:border-box}
.zoombox .pctin:focus{outline:2px solid #22c55e;border-radius:4px;background:#f0fdf4}
.chart-wrap{overflow:auto;border:1px solid #e5e7eb;border-radius:8px;background:#fcfdfc;height:clamp(360px,80vh,1100px)}
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
.chart-cell:target .chart-node{outline:3px solid #f59e0b;outline-offset:2px;box-shadow:0 0 0 4px rgba(245,158,11,.25)}
.search-form{display:flex;gap:8px;align-items:flex-start}
.search-form input{flex:1;width:auto}
.search-result{display:block;border:1px solid #bbf7d0;background:#f0fdf4;border-radius:8px;padding:9px 11px;margin:6px 0;color:#14532d;text-decoration:none}
.search-result:hover{background:#dcfce7}
.chart-note{margin-top:4px}
.chart-drill{display:inline-block;margin-top:5px;background:#fff;color:#14532d;border:1.5px solid #14532d;border-radius:6px;padding:4px 10px;font-size:12.5px;font-weight:700;text-decoration:none}
.chart-drill:hover{background:#dcfce7}
.chart-edit{display:inline-block;margin-top:4px;color:#14532d;font-size:12px;font-weight:600}
.chart-edit:hover{color:#166534;text-decoration-thickness:2px}
.chart-root .chart-edit{color:#dcfce7}
.chart-root .chart-edit:hover{color:#ffffff}
.chart-delete{display:inline-block;margin-top:4px;margin-left:10px;color:#b91c1c;font-size:12px;font-weight:600}
.chart-delete:hover{color:#991b1b;text-decoration-thickness:2px}
.chart-focus-link{display:inline-block;margin-top:4px;color:#1d4ed8;font-size:12px;font-weight:600}
.chart-focus-link:hover{color:#1e40af;text-decoration-thickness:2px}
.crumbs{font-size:13.5px;margin:8px 0}.crumbs a{color:#14532d;font-weight:600}

@media print{
@page{size:landscape;margin:10mm}
body{background:#fff}
header,.nav,.chart-bar,.flash,form[action="/logout"]{display:none !important}
.card{display:none !important}
.card.chart-card{display:block !important;border:none;padding:0;margin:0}
.card.chart-card p.small{display:none !important}
.breakout{width:auto !important;margin-left:0 !important;transform:none !important}
.chart-wrap{overflow:visible !important;height:auto !important;max-height:none !important;border:none !important;background:#fff}
.chart-wrap *{print-color-adjust:exact;-webkit-print-color-adjust:exact}
.chart-lvl{position:static !important}
.chart-node{font-size:11px;line-height:1.4}
.chart-node .small{font-size:9px}
.chart-edit,.chart-delete,.chart-focus-link,.chart-drill{display:none !important}
form.chart-slot input,form.chart-slot button{display:none !important}
}
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
  const introPart = m.introducer ? ` / 紹介:${esc(m.introducer)}` : '';
  const label = m.root
    ? `<span class="node root-node">${esc(m.name)}</span>`
    : `<span class="node"><strong>${esc(m.name)}</strong> <span class="small">ID:${esc(m.memberId)} / 登録:${esc(m.regDate || '-')}${introPart} / 左${c.left}・右${c.right}</span></span>`;
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
// 見せる。明示指定がある場合はデータより深くても指定段数で打ち切り、隠れた部分は
// 省略注記と「＋ 下を増やす」フォーカス導線からたどれるようにする。指定がない場合は
// 従来どおり、データの深さに合わせて CHART_MAX_DEPTH まで表示する。
const CHART_MAX_DEPTH = 10;
const CHART_COL_W = 140;
function collectChartItems(rootId, maxDepth = CHART_MAX_DEPTH) {
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
    if (depth >= maxDepth) {
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
  if (m.root) {
    // 会社（一番上）の枠は、本部の編集モードでのみ「名前を修正」を出す（削除リンクは
    // 出さない）。リーダー画面には会社の枠自体が描かれないため、リーダー側に影響はない。
    const rootEdit = editHref ? `<br><a class="chart-edit" href="${editHref}">名前を修正</a>` : '';
    return `<div class="chart-node chart-root">${esc(m.name)}${rootEdit}</div>`;
  }
  const c = countsFor(m.memberId);
  const editLink = editHref ? `<br><a class="chart-edit" href="${editHref}">名前を修正</a>` : '';
  const deleteLink = deleteHref ? `<a class="chart-delete" href="${deleteHref}">削除</a>` : '';
  const focusLink = focusHref ? `<br><a class="chart-focus-link" href="${focusHref}">この人から下を見る</a>` : '';
  const introLine = m.introducer ? `<br>紹介:${esc(m.introducer)}` : '';
  return `<div class="chart-node"><strong>${esc(m.name)}</strong><br><span class="small">ID:${esc(m.memberId)}<br>登録:${esc(m.regDate || '-')}${introLine}<br>左${c.left}・右${c.right}</span>${editLink}${deleteLink}${focusLink}</div>`;
}
// 空き枠セルのアンカーID。HTML id として安全な文字だけを使い、それ以外は '-' に置き換える。
function slotAnchorId(parentId, side) {
  return 'slot-' + String(parentId).replace(/[^A-Za-z0-9_-]/g, '-') + '-' + side;
}
// 空き枠の中に置く、その場で登録できる小さなフォーム。
// 登録後の戻り先で表示段数を維持するため、現在の表示段数（levels）も隠しフィールドで送る。
// 枝フォーカス表示のときは focus も隠しフィールドで送り、登録後も同じ枝に戻れるようにする
// （フォーカス表示でないときは focus フィールド自体を出さない＝従来のマークアップのまま）。
// from=chart は「図の中のフォームから」という目印。登録後の戻り先URLに、登録した枠の
// アンカー（#slot-…）を付けて作業していた場所へ戻すために使う（下の汎用フォームには付けない）。
function chartSlotForm(basePath, slot, today, levels, focusId) {
  const sideLabel = slot.side === 'L' ? '左' : '右';
  return `<form class="chart-slot" method="post" action="${basePath}/add"><div class="slot-title">＋ ${sideLabel}に追加</div>`
    + `<input type="text" name="name" placeholder="名前" maxlength="100">`
    + `<input type="date" name="regDate" value="${today}">`
    + `<input type="text" name="introducer" placeholder="紹介者（必須）" maxlength="100" required>`
    + `<input type="hidden" name="parentId" value="${esc(slot.parentId)}">`
    + `<input type="hidden" name="side" value="${slot.side}">`
    + `<input type="hidden" name="levels" value="${levels}">`
    + `<input type="hidden" name="from" value="chart">`
    + (focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : '')
    + `<button type="submit">登録</button></form>`;
}
// focusCtx: null（担当トップを先頭に表示）か {focusId, levelOffset}（枝フォーカス表示）。
// 枝フォーカス表示では、指定会員を図の先頭にして、そこから先を新たに CHART_MAX_DEPTH 段
// まで描く。上限段の会員には「＋ 下を増やす」リンク（?focus=<会員ID>）を付け、その会員を
// 先頭に開き直せるようにすることで、深い枝を何度でもたどれる（単独版 space-2 の
// 「10段目以降は枝ごとに伸ばす」ハイブリッド方式と同じ考え方）。段ラベルは
// levelOffset を加えて、担当範囲全体での実際の段数で表示する。
// opts.viewOnly: 見る専用モード。空き枠はフォームにせず薄い「空き」枠のままにし、
// 会員の箱に修正・削除リンクを付けない。
// opts.hqEditable（本部のツリー表示用）: 本部が図の中で追加・修正・削除できる編集
// モード。空き枠は basePath（/hq/tree）へ送る入力フォームにし、会員の箱には修正・
// 削除リンクを付ける。加えて、会社以外の全会員の箱に、その会員を先頭に開く
// 「この人から下を見る」リンクを付ける。
// フォーカス（下を増やす）リンクは basePath に対して付くので、本部用は /hq/tree への
// ドリルダウンになる。
function renderChartGrid(rootId, basePath, reqLevels, focusCtx, opts) {
  const today = todayJst();
  const viewOnly = !!(opts && opts.viewOnly);
  const hqEditable = !!(opts && opts.hqEditable);
  // リーダー画面から渡される担当トップの会員ID。その会員の箱には削除リンクを付けない。
  const leaderRootId = opts && opts.leaderRootId ? opts.leaderRootId : null;
  // 明示指定された表示段数は、データがそれより深い場合でもその段数で打ち切る。
  // そのため収集も表示最終段（相対深さ reqLevels - 1）までに制限し、最終段の会員を
  // 既存の上限段と同じ cutoff / omitted として扱う。指定がない場合は従来どおり
  // CHART_MAX_DEPTH まで収集する。
  const collectionMaxDepth = reqLevels != null
    ? Math.min(CHART_MAX_DEPTH, reqLevels - 1)
    : CHART_MAX_DEPTH;
  const { byDepth, omitted, cutoff } = collectChartItems(rootId, collectionMaxDepth);
  const focusId = focusCtx ? focusCtx.focusId : null;
  const levelOffset = focusCtx ? focusCtx.levelOffset : 0;
  const depths = [...byDepth.keys()].sort((a, b) => a - b);
  const floorDepth = depths.length ? depths[depths.length - 1] : 0;
  const floorLevels = floorDepth + 1;
  // 表示段数：明示指定があればその値をそのまま使う。指定がない場合はデータの段数。
  let displayLevels = floorLevels;
  if (reqLevels != null) displayLevels = reqLevels;
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
          // 見る専用モードでは修正リンクを付けない（本部の編集モードでは付ける）。
          let editHref = '';
          if (!viewOnly) {
            editHref = `${basePath}?edit=${encodeURIComponent(it.m.memberId)}`;
            if (focusId) editHref += `&focus=${encodeURIComponent(focusId)}`;
            if (reqLevels != null) editHref += `&levels=${reqLevels}`;
            editHref += '#edit-form';
          }
          // 編集できる画面では、リーダーの担当トップ以外の会員の箱に「削除」リンクも
          // 付ける（削除の確認カードへ移動するだけ。実際に消せるかはサーバー側の
          // 共通ガードで判定する。本部の編集モードでは leaderRootId が無いため、
          // 会社以外の会員が対象になる）。
          let deleteHref = '';
          if (!viewOnly && it.m.memberId !== leaderRootId && !it.m.root) {
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
          // 会社以外の全会員の箱から、その会員を先頭にした図へ直接移動できる
          // 「この人から下を見る」リンクを付ける（本部・リーダー共通。2026-11-08に
          // 本人の希望でリーダー画面にも付けるよう変更）。リーダー画面に描かれるのは
          // 担当範囲内の会員だけなので範囲外へのリンクは出ず、サーバー側でも
          // フォーカス先が担当範囲内かを確認している。
          const focusHref = !it.m.root
            ? `${basePath}?focus=${encodeURIComponent(it.m.memberId)}#chartWrap`
            : '';
          // 会員の箱にも、その会員が入っている枠のアンカーID（slot-<親>-<左右>）を付ける。
          // 空き枠だったときと同じIDなので、登録直後にその枠へ戻るURL（#slot-…）が
          // 登録されたばかりの会員の箱を指すようになる。会社の枠（parentIdなし）は付けない。
          const cellId = it.m.parentId ? ` id="${slotAnchorId(it.m.parentId, it.m.side)}"` : '';
          cells += `<div class="chart-cell"${cellId} style="${cellStyle(d, it.pos)}">${chartNodeHtml(it.m, editHref, focusHref, deleteHref)}${note}${drill}</div>`;
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
  <label>紹介者（必須）<input name="introducer" required maxlength="100"></label>
  <label>上に付く人（親）<select name="parentId">${memberOptions(pre.parentId || '', scopeIds)}</select></label>
  <label>位置<select name="side"><option value="L" ${pre.side === 'L' ? 'selected' : ''}>左</option><option value="R" ${pre.side === 'R' ? 'selected' : ''}>右</option></select></label>
  <button type="submit">登録する</button></form>`;
}

// ---------- 本部 ----------
function hqNav() {
  return `<p class="nav"><a href="/hq">組織図（全体）</a><a href="/hq/tree">組織図（ツリー）</a><a href="/hq/members">会員一覧</a><a href="/hq/leaders">リーダー管理</a><a href="/hq/conflicts">要確認リスト</a><a href="/hq/export.csv">CSV書き出し</a><a href="/hq/export.xlsx">Excel書き出し</a></p>
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
// 全体表示し、本部が図の中でそのまま追加・修正・削除できるページ。空き枠は入力
// フォームにし、会員の箱には修正・削除リンクを付ける（会社の枠は名前だけ変更可・削除不可）。
// 深い組織のために、リーダー画面と同じ枝フォーカス（?focus=）が使える。
async function handleHqTree(req, res, url) {
  if (!isHq(req)) return redirect(res, '/login');
  const basePath = '/hq/tree';
  if (req.method === 'POST') {
    const action = url.pathname.endsWith('/add') ? 'add'
      : url.pathname.endsWith('/edit') ? 'edit'
      : url.pathname.endsWith('/delete') ? 'delete'
      : null;
    if (!action) return send(res, 404, page('見つかりません', '<div class="card"><h2>ページが見つかりません</h2></div>'));
    const body0 = await readBody(req);
    const fields = Object.fromEntries(body0);
    const r = action === 'add'
      ? await addMember({ type: 'hq' }, fields)
      : action === 'edit'
        ? await updateMemberByHq(fields)
        : await deleteMemberByHq(fields);
    // 戻り先では、実在する会社以外の会員へのフォーカスと、2〜10の表示段数だけを
    // 引き継ぐ。削除した会員自身がフォーカス先だった場合は、削除後に実在しないため
    // 引き継がれず、全体表示に戻る。
    const rawFocus = String(body0.get('focus') || '').trim();
    const focusPart = rawFocus && rawFocus !== ROOT_ID && store.members[rawFocus]
      ? `focus=${encodeURIComponent(rawFocus)}&`
      : '';
    const lvPart = leaderLevelsPart(body0.get('levels'));
    // 図の中の空き枠フォームからの追加では、リーダー画面と同じく、追加した枠の
    // アンカーを戻り先に付けて作業していた場所がそのまま見えるようにする。
    const anchorPart = action === 'add' && String(body0.get('from') || '') === 'chart' && body0.get('parentId') && body0.get('side')
      ? '#' + slotAnchorId(body0.get('parentId'), body0.get('side'))
      : '';
    const key = r.result === 'error' ? 'err' : 'msg';
    return redirect(res, `${basePath}?${focusPart}${lvPart}${key}=${encodeURIComponent(r.message)}${anchorPart}`);
  }
  // 表示する段数（?levels=）。2〜10に丸め、指定があればデータより深くてもその段数で表示する（リーダー画面と同じ）。
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
  const chart = renderChartGrid(chartTopId, basePath, reqLevels, focusId ? { focusId, levelOffset } : null, { hqEditable: true });
  const lvMinus = Math.max(2, chart.levels - 1);
  const lvPlus = chart.levels < CHART_MAX_DEPTH ? chart.levels + 1 : chart.levels;
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
  // 修正・削除カードのキャンセルで、見ていたツリー表示（フォーカス・段数）へ戻す。
  const viewParams = [];
  if (focusId) viewParams.push(`focus=${encodeURIComponent(focusId)}`);
  if (reqLevels != null) viewParams.push(`levels=${reqLevels}`);
  const currentViewHref = viewParams.length ? `${basePath}?${viewParams.join('&')}` : basePath;
  // 会員情報の修正（?edit=<会員ID>）。実在する会員を対象にし、会社の枠は名前だけの
  // 変更カードを出す。
  const editParam = (url.searchParams.get('edit') || '').trim();
  const editMember = editParam && store.members[editParam] ? store.members[editParam] : null;
  let editCard = '';
  if (editMember && editMember.root) {
    editCard = `<div class="card" id="edit-form"><h3>一番上の名前を変更する</h3>
  <p class="small">組織図の一番上に出る名前です。変更できるのは名前だけで、この枠そのものは削除できません。</p>
  <form method="post" action="${basePath}/edit">
  <label>名前<input name="name" value="${esc(editMember.name)}" required maxlength="100"></label>
  <input type="hidden" name="memberId" value="${esc(editMember.memberId)}">
  ${focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : ''}
  ${reqLevels != null ? `<input type="hidden" name="levels" value="${reqLevels}">` : ''}
  <button type="submit">変更を保存</button> <a class="btn sub" href="${currentViewHref}">キャンセル</a></form></div>`;
  } else if (editMember) {
    editCard = `<div class="card" id="edit-form"><h3>会員情報を修正する</h3>
  <p class="small">対象：${esc(editMember.name)}（${esc(editMember.memberId)}）</p>
  <form method="post" action="${basePath}/edit">
  <label>名前<input name="name" value="${esc(editMember.name)}" required maxlength="100"></label>
  <label>登録日<input name="regDate" type="date" value="${esc(editMember.regDate || '')}"></label>
  <label>紹介者（必須）<input name="introducer" value="${esc(editMember.introducer || '')}" required maxlength="100"></label>
  <input type="hidden" name="memberId" value="${esc(editMember.memberId)}">
  ${focusId ? `<input type="hidden" name="focus" value="${esc(focusId)}">` : ''}
  ${reqLevels != null ? `<input type="hidden" name="levels" value="${reqLevels}">` : ''}
  <button type="submit">変更を保存</button> <a class="btn sub" href="${currentViewHref}">キャンセル</a></form></div>`;
  }
  // 会員の削除（?delete=<会員ID>）。実在する会社以外の会員だけを確認対象にし、
  // 共通ガードで消せない場合は理由だけを表示して削除フォーム自体を出さない。
  const deleteParam = (url.searchParams.get('delete') || '').trim();
  const deleteTarget = deleteParam && deleteParam !== ROOT_ID && store.members[deleteParam] && !store.members[deleteParam].root
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
  <button class="danger" type="submit">削除する</button> <a class="btn sub" href="${currentViewHref}">キャンセル</a></form>`;
    deleteCard = `<div class="card" id="delete-form"><h3>会員を削除する</h3>
  <p class="small">対象：${esc(deleteTarget.name)}（${esc(deleteTarget.memberId)}） / 登録：${esc(deleteTarget.regDate || '-')} / 親：${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')} の${deleteTarget.side === 'L' ? '左' : deleteTarget.side === 'R' ? '右' : ''}</p>
  ${actionHtml}</div>`;
  }
  // 会員検索（?q=）。リーダー画面と同じく、全体から名前・会員IDの部分一致で探し、
  // 結果を押すとその会員を先頭にした図（フォーカス表示）へ移動する。
  const searchQuery = (url.searchParams.get('q') || '').trim();
  let searchResultsHtml = '';
  if (searchQuery) {
    const needle = searchQuery.toLowerCase();
    const depthMap = scopeDepthMap(ROOT_ID);
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
        const parent = m.root
          ? 'トップ'
          : (m.parentId && store.members[m.parentId] ? store.members[m.parentId].name : '');
        const href = m.root
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
  const body = `${hqNav()}
  ${searchCard}
  <div class="card breakout chart-card"><h2>組織図（ツリー表示・会社全体）</h2>
  <p class="small">会社全体の組織図です。空いている場所の枠に名前を入力して「登録」を押すと、その場所にそのまま登録できます（登録日には今日の日付が入っています。変える場合は日付を変更してください）。紹介者は必須です。必ず入力してください。</p>
  <p class="small">各会員の箱の「名前を修正」から名前・登録日・紹介者を直せます。一番上の枠も「名前を修正」から名前だけ変更できます。「削除」は、下に人がいる場合など、組織を壊すおそれがある場合はできません（理由は確認画面に表示されます）。</p>
  <p class="small">「表示する段数」の＋を押すと、まだ誰もいない先の段まで図を広げて見られます。先の段の「空き」を押すと、その枝の登録できる空き枠へ移動します。各会員の「この人から下を見る」を押すと、その人を先頭にした図へ移動できます。図の一番下の段にいる会員の「＋ 下を増やす」を押すと、その人を先頭にして、さらに下の枝を見たり入力したりできます（何度でも繰り返せます）。</p>
  ${focusBanner}
  <div class="chart-bar"><span class="zlabel">表示する段数：</span><span class="zoombox"><a class="zbtn" href="${lvHref(lvMinus)}" aria-label="段数を減らす">−</a><input class="pctin" id="lvIn" type="number" min="2" max="10" step="1" value="${chart.levels}" data-href="${lvHref('__N__')}" aria-label="表示する段数">段<a class="zbtn" href="${lvHref(lvPlus)}" aria-label="段数を増やす">＋</a></span><span class="zlabel">図の大きさ：</span><span class="zoombox"><button type="button" id="zOut" aria-label="縮小">−</button><input class="pctin" id="zPctIn" type="number" min="2" max="200" step="1" value="100" aria-label="図の大きさ（パーセント）">%<button type="button" id="zIn" aria-label="拡大">＋</button><button type="button" id="zFit">等倍</button><button type="button" id="zFitAll">全体表示</button><button type="button" id="zPrint">印刷</button></span><span class="zlabel">横スクロール・ピンチで拡大縮小できます</span></div>
  <div class="chart-wrap" id="chartWrap"><div class="chart-box" id="chartBox"><div class="chart-scale" id="chartScale">${chart.html}</div></div></div></div>
  ${editCard}
  ${deleteCard}
  <script>(function(){var z=1;var scale=document.getElementById('chartScale');var box=document.getElementById('chartBox');var pct=document.getElementById('zPctIn');var wrap=document.getElementById('chartWrap');if(!scale||!box||!pct||!wrap)return;function apply(){scale.style.transform='scale('+z+')';box.style.width=Math.ceil(scale.scrollWidth*z)+'px';box.style.height=Math.ceil(scale.scrollHeight*z)+'px';if(document.activeElement!==pct)pct.value=Math.round(z*100);}function zoomAt(nz,cx,cy){nz=Math.min(2,Math.max(0.02,Math.round(nz*100)/100));if(nz===z)return;var r=wrap.getBoundingClientRect();var fx=(cx===undefined?r.left+r.width/2:cx)-r.left;var fy=(cy===undefined?r.top+r.height/2:cy)-r.top;var ux=(wrap.scrollLeft+fx)/z;var uy=(wrap.scrollTop+fy)/z;z=nz;apply();wrap.scrollLeft=Math.round(ux*z-fx);wrap.scrollTop=Math.round(uy*z-fy);}document.getElementById('zIn').addEventListener('click',function(){zoomAt(z*1.25);});document.getElementById('zOut').addEventListener('click',function(){zoomAt(z/1.25);});document.getElementById('zFit').addEventListener('click',function(){zoomAt(1);});document.getElementById('zFitAll').addEventListener('click',function(){fitAll();});document.getElementById('zPrint').addEventListener('click',function(){window.print();});pct.addEventListener('change',function(){var v=parseFloat(pct.value);if(isNaN(v)){pct.value=Math.round(z*100);return;}zoomAt(Math.min(200,Math.max(2,v))/100);pct.value=Math.round(z*100);});pct.addEventListener('keydown',function(e){if(e.key==='Enter'){e.preventDefault();pct.blur();}});var lvIn=document.getElementById('lvIn');if(lvIn){var goLv=function(){var n=parseInt(lvIn.value,10);if(isNaN(n))return;n=Math.min(10,Math.max(2,n));var t=lvIn.getAttribute('data-href');if(t)location.href=t.replace('__N__',n);};lvIn.addEventListener('change',goLv);lvIn.addEventListener('keydown',function(e){if(e.key==='Enter'){e.preventDefault();goLv();}});}function fitAll(){var w=scale.scrollWidth,h=scale.scrollHeight;var aw=wrap.clientWidth-2,ah=wrap.clientHeight-2;if(w>0&&h>0&&aw>0&&ah>0){z=Math.max(0.02,Math.round(Math.min(1,Math.min(aw/w,ah/h))*1000)/1000);apply();wrap.scrollLeft=0;wrap.scrollTop=0;}}function centerTop(){var w=Math.ceil(scale.scrollWidth*z);var aw=wrap.clientWidth;wrap.scrollLeft=w>aw?Math.round((w-aw)/2):0;wrap.scrollTop=0;}function prepPrint(){var w=scale.scrollWidth;var aw=Math.min(wrap.clientWidth-2,1080);if(w>0&&aw>0){var pz=Math.max(0.02,Math.min(1,aw/w));scale.style.transform='none';scale.style.zoom=pz;box.style.width='auto';box.style.height='auto';}}var savedView=null;window.addEventListener('beforeprint',function(){if(savedView)return;savedView={z:z,sl:wrap.scrollLeft,st:wrap.scrollTop};prepPrint();});window.addEventListener('afterprint',function(){if(!savedView)return;scale.style.zoom='';z=savedView.z;apply();wrap.scrollLeft=savedView.sl;wrap.scrollTop=savedView.st;savedView=null;});var pd=0;function tdist(e){var a=e.touches[0],b=e.touches[1];var dx=a.clientX-b.clientX,dy=a.clientY-b.clientY;return Math.sqrt(dx*dx+dy*dy);}function tmid(e){var a=e.touches[0],b=e.touches[1];return[(a.clientX+b.clientX)/2,(a.clientY+b.clientY)/2];}wrap.addEventListener('touchstart',function(e){if(e.touches.length===2)pd=tdist(e);},{passive:true});wrap.addEventListener('touchmove',function(e){if(e.touches.length===2&&pd>0){e.preventDefault();var d=tdist(e);var m=tmid(e);zoomAt(z*d/pd,m[0],m[1]);pd=d;}},{passive:false});wrap.addEventListener('touchend',function(e){if(e.touches.length<2)pd=0;});wrap.addEventListener('touchcancel',function(){pd=0;});wrap.addEventListener('wheel',function(e){if(e.ctrlKey){e.preventDefault();zoomAt(z*(e.deltaY<0?1.1:0.9),e.clientX,e.clientY);}},{passive:false});apply();var hId=location.hash&&location.hash.length>1?location.hash.slice(1):'';var hEl=hId?document.getElementById(hId):null;if(hEl&&hEl.id!=='chartWrap'&&scale.contains(hEl)){var hr=hEl.getBoundingClientRect();var wr=wrap.getBoundingClientRect();wrap.scrollLeft+=Math.round((hr.left+hr.width/2)-(wr.left+wrap.clientWidth/2));wrap.scrollTop+=Math.round((hr.top+hr.height/2)-(wr.top+wrap.clientHeight/2));}else{centerTop();}})();</script>`;
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
  if (q) list = list.filter(m => m.name.toLowerCase().includes(q) || m.memberId.toLowerCase().includes(q) || (m.introducer || '').toLowerCase().includes(q));
  list.sort((a, b) => (a.root ? 0 : 1) - (b.root ? 0 : 1) || a.memberId.localeCompare(b.memberId));
  const memberCount = list.filter(m => !m.root).length;
  const rows = list.map(m => {
    const c = countsFor(m.memberId);
    const parent = m.parentId ? store.members[m.parentId] : null;
    const treeHref = m.root ? '/hq/tree' : `/hq/tree?focus=${encodeURIComponent(m.memberId)}`;
    const deleteCell = m.root ? '<td></td>' : `<td><a href="/hq/members?delete=${encodeURIComponent(m.memberId)}#delete-form">削除</a></td>`;
    return `<tr><td>${esc(m.memberId)}</td><td>${esc(m.name)}</td><td>${esc(m.introducer || '')}</td><td>${esc(m.regDate || '')}</td><td>${esc(parent ? parent.name + '（' + parent.memberId + '）' : '')}</td><td>${m.side === 'L' ? '左' : m.side === 'R' ? '右' : ''}</td><td>${c.left}</td><td>${c.right}</td><td>${esc(m.enteredBy || '')}</td><td><a href="${treeHref}">ツリーで見る</a></td>${deleteCell}</tr>`;
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
  <form method="get" action="/hq/members"><input name="q" value="${esc(q)}" placeholder="名前・会員ID・紹介者で検索"><button type="submit">検索</button></form>
  <table><tr><th>会員ID</th><th>名前</th><th>紹介者</th><th>登録日</th><th>親</th><th>位置</th><th>左人数</th><th>右人数</th><th>入力者</th><th>ツリー</th><th>削除</th></tr>${rows || '<tr><td colspan="11">該当なし</td></tr>'}</table></div>
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
    if (action === 'reissue') {
      if (!store.members[leader.rootMemberId]) return redirect(res, back('/hq/leaders', 'err', '担当トップの会員が存在しないため、再発行できません。'));
      leader.token = newToken(); leader.status = 'active'; await saveStore(); return redirect(res, back('/hq/leaders', 'msg', '専用リンクを再発行しました（前のリンクは使えません）。'));
    }
    if (action === 'revoke') { leader.status = 'revoked'; await saveStore(); return redirect(res, back('/hq/leaders', 'msg', '専用リンクを無効化しました。')); }
    if (action === 'delete') {
      // リーダー自体の削除（間違えて作ったリーダーの後片付け用）。確認画面を経た
      // POSTでのみ実行する。消すと専用リンクは即座に使えなくなり、一覧からも消える。
      store.leaders = store.leaders.filter(l => l.id !== leader.id);
      await saveStore();
      return redirect(res, back('/hq/leaders', 'msg', `リーダー「${leader.name}」を削除しました（専用リンクは使えなくなりました）。`));
    }
    return redirect(res, '/hq/leaders');
  }
  const base = `${req.headers['x-forwarded-proto'] ? String(req.headers['x-forwarded-proto']).split(',')[0] : 'http'}://${req.headers.host}`;
  const rows = store.leaders.map(l => {
    const root = store.members[l.rootMemberId];
    const link = l.status === 'active' ? `${base}/leader/${l.token}` : '（無効化済み）';
    return `<tr><td>${esc(l.name)}</td><td>${esc(root ? root.name + '（' + root.memberId + '）' : l.rootMemberId)}</td>
    <td style="word-break:break-all">${esc(link)}</td><td>${l.status === 'active' ? '有効' : '無効'} / ${l.consentAt ? '同意済み' : '未同意'}</td>
    <td><form method="post" action="/hq/leaders" style="display:inline"><input type="hidden" name="leaderId" value="${esc(l.id)}"><input type="hidden" name="action" value="reissue"><button class="sub" type="submit">再発行</button></form>
    <form method="post" action="/hq/leaders" style="display:inline"><input type="hidden" name="leaderId" value="${esc(l.id)}"><input type="hidden" name="action" value="revoke"><button class="danger" type="submit">無効化</button></form>
    <a href="/hq/leaders?delete=${esc(l.id)}#leader-delete">削除</a></td></tr>`;
  }).join('');
  // リーダーの削除確認（?delete=<リーダーID>）。対象があるときだけ確認カードを出す。
  // 実際の削除は POST（action=delete）でのみ行う。
  const delParam = (url.searchParams.get('delete') || '').trim();
  const delLeader = delParam ? store.leaders.find(l => l.id === delParam) : null;
  let leaderDeleteCard = '';
  if (delLeader) {
    const root = store.members[delLeader.rootMemberId];
    leaderDeleteCard = `<div class="card" id="leader-delete"><h3>リーダーを削除する</h3>
  <p class="small">対象：${esc(delLeader.name)}（担当トップ：${esc(root ? root.name + '（' + root.memberId + '）' : delLeader.rootMemberId)}）</p>
  <p class="small">削除すると、このリーダーの専用リンクはすぐに使えなくなり、一覧からも消えます。元に戻せません。間違えて作ったリーダーだけを削除してください。</p>
  <form method="post" action="/hq/leaders"><input type="hidden" name="leaderId" value="${esc(delLeader.id)}"><input type="hidden" name="action" value="delete"><button class="danger" type="submit">削除する</button> <a class="btn sub" href="/hq/leaders">キャンセル</a></form></div>
  `;
  }
  // 担当トップの検索（?mq=）。会員が増えるとプルダウンから探すのが大変なため、
  // 名前・会員IDの部分一致で候補を絞ってから選べるようにする。検索しない場合は
  // 従来どおり全会員をプルダウンに出す。候補は最大50件（会社の枠は対象外）。
  const mq = (url.searchParams.get('mq') || '').trim();
  let rootOptions = memberOptions('', null);
  let rootSearchNote = '';
  if (mq) {
    const needle = mq.toLowerCase();
    const matches = Object.values(store.members)
      .filter(m => !m.root && (m.name.toLowerCase().includes(needle) || m.memberId.toLowerCase().includes(needle)))
      .sort((a, b) => a.memberId.localeCompare(b.memberId));
    const shown = matches.slice(0, 50);
    rootOptions = shown.map(m => `<option value="${esc(m.memberId)}">${esc(m.name)}（${esc(m.memberId)}）</option>`).join('');
    rootSearchNote = matches.length
      ? `<p class="small">「${esc(mq)}」の検索結果：${matches.length}件${matches.length > shown.length ? `（最初の${shown.length}件を表示しています）` : ''}。下の「担当トップ」から選んでください。</p>`
      : `<p class="small">「${esc(mq)}」に当たる会員は見つかりませんでした。名前や会員IDを変えて検索してください。</p>`;
  }
  const body = `${hqNav()}${leaderDeleteCard}<div class="card"><h2>リーダー管理</h2>
  <p class="small">専用リンクをリーダーに渡してください。リンクを開くとパスワードなしで、そのリーダーの配下だけが見える画面に入ります。再発行すると前のリンクは使えなくなります。</p>
  <table><tr><th>リーダー名</th><th>担当トップ（ルート会員）</th><th>専用リンク</th><th>状態</th><th>操作</th></tr>${rows || '<tr><td colspan="5">まだいません</td></tr>'}</table></div>
  <div class="card"><h3>リーダーを作る</h3>
  <form method="get" action="/hq/leaders" class="search-form">
  <label>担当トップを検索（名前・会員IDの一部で探せます）<input name="mq" value="${esc(mq)}" placeholder="例：山田 / 161"></label>
  <button class="sub" type="submit">検索</button></form>
  ${rootSearchNote}
  <form method="post" action="/hq/leaders">
  <input type="hidden" name="action" value="create">
  <label>リーダー名<input name="name" required></label>
  <label>担当トップ（この会員から下が、そのリーダーの担当範囲です）<select name="rootMemberId">${rootOptions}</select></label>
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
        Object.assign(cur, { name: claimed.name, parentId: claimed.parentId, side: claimed.side, regDate: claimed.regDate, introducer: claimed.introducer !== undefined ? claimed.introducer : (cur.introducer || ''), enteredBy: c.submittedByName + '（本部が採用）' });
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
function memberExportData() {
  // 会員IDの数字順に並べる（文字列順だと 1, 10, 100... になって読みにくいため）
  const list = Object.values(store.members).filter(m => !m.root).sort((a, b) => {
    const na = /^\d+$/.test(a.memberId) ? BigInt(a.memberId) : null;
    const nb = /^\d+$/.test(b.memberId) ? BigInt(b.memberId) : null;
    if (na !== null && nb !== null) return na < nb ? -1 : na > nb ? 1 : 0;
    if (na !== null) return -1;
    if (nb !== null) return 1;
    return String(a.memberId).localeCompare(String(b.memberId));
  });
  const header = ['会員ID', '名前', '紹介者', '登録日', '親の会員ID', '位置', '左人数', '右人数', '配下合計'];
  const rows = list.map(m => {
    const c = countsFor(m.memberId);
    return [m.memberId, m.name, m.introducer || '', m.regDate || '', m.parentId || '', m.side === 'L' ? '左' : m.side === 'R' ? '右' : '', c.left, c.right, c.total];
  });
  return { header, rows };
}
function handleExportCsv(req, res) {
  if (!isHq(req)) return redirect(res, '/login');
  const { header, rows } = memberExportData();
  const quote = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const lines = [header.join(',')];
  for (const row of rows) lines.push(row.map(quote).join(','));
  const csv = '﻿' + lines.join('\r\n') + '\r\n';
  res.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': 'attachment; filename="members.csv"' });
  res.end(csv);
}
// ---- Excel (.xlsx) 生成（外部ライブラリなし。ZIP+XMLを自前で組む） ----
const CRC_TABLE = (() => { const t = new Int32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; t[n] = c; } return t; })();
function crc32(buf) { let c = 0xFFFFFFFF; for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xFF] ^ (c >>> 8); return (c ^ 0xFFFFFFFF) >>> 0; }
function zipDeflate(files) {
  const chunks = []; const central = []; let offset = 0;
  for (const f of files) {
    const nameBuf = Buffer.from(f.name, 'utf8');
    const comp = zlib.deflateRawSync(f.data, { level: 6 });
    const crc = crc32(f.data);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4); local.writeUInt16LE(0x0800, 6); local.writeUInt16LE(8, 8);
    local.writeUInt32LE(crc, 14); local.writeUInt32LE(comp.length, 18); local.writeUInt32LE(f.data.length, 22); local.writeUInt16LE(nameBuf.length, 26);
    chunks.push(local, nameBuf, comp);
    const cen = Buffer.alloc(46);
    cen.writeUInt32LE(0x02014b50, 0); cen.writeUInt16LE(20, 4); cen.writeUInt16LE(20, 6); cen.writeUInt16LE(0x0800, 8); cen.writeUInt16LE(8, 10);
    cen.writeUInt32LE(crc, 16); cen.writeUInt32LE(comp.length, 20); cen.writeUInt32LE(f.data.length, 24); cen.writeUInt16LE(nameBuf.length, 28); cen.writeUInt32LE(offset, 42);
    central.push(cen, nameBuf);
    offset += 30 + nameBuf.length + comp.length;
  }
  const cenSize = central.reduce((sum, b) => sum + b.length, 0);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(files.length, 8); end.writeUInt16LE(files.length, 10);
  end.writeUInt32LE(cenSize, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...chunks, ...central, end]);
}
function xlsxEscape(v) {
  return String(v == null ? '' : v).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function xlsxColName(i) { let s = ''; let n = i + 1; while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; }
function buildXlsx(header, rows) {
  const widths = [10, 32, 32, 13, 12, 8, 10, 10, 10];
  const cellXml = (v, ref, bold) => {
    const style = bold ? ' s="1"' : '';
    if (typeof v === 'number' && Number.isFinite(v)) return `<c r="${ref}"${style}><v>${v}</v></c>`;
    if (/^\d+$/.test(String(v)) && String(v).length <= 15) return `<c r="${ref}"${style}><v>${String(v)}</v></c>`;
    return `<c r="${ref}"${style} t="inlineStr"><is><t xml:space="preserve">${xlsxEscape(v)}</t></is></c>`;
  };
  const rowXml = (vals, rIdx, bold) => `<row r="${rIdx}">` + vals.map((v, cIdx) => cellXml(v, xlsxColName(cIdx) + rIdx, bold)).join('') + '</row>';
  const sheetRows = [rowXml(header, 1, true)];
  rows.forEach((row, i) => sheetRows.push(rowXml(row, i + 2, false)));
  const lastRef = xlsxColName(header.length - 1) + (rows.length + 1);
  const sheet = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft" activeCell="A2" sqref="A2"/></sheetView></sheetViews><sheetFormatPr defaultRowHeight="15"/><cols>${widths.map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w}" customWidth="1"/>`).join('')}</cols><sheetData>${sheetRows.join('')}</sheetData><autoFilter ref="A1:${lastRef}"/></worksheet>`;
  const contentTypes = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`;
  const rels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`;
  const workbook = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="会員一覧" sheetId="1" r:id="rId1"/></sheets></workbook>`;
  const workbookRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`;
  const styles = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles><cellXfs count="2"><xf fontId="0" fillId="0" borderId="0" xfId="0"/><xf fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs></styleSheet>`;
  return zipDeflate([
    { name: '[Content_Types].xml', data: Buffer.from(contentTypes, 'utf8') },
    { name: '_rels/.rels', data: Buffer.from(rels, 'utf8') },
    { name: 'xl/workbook.xml', data: Buffer.from(workbook, 'utf8') },
    { name: 'xl/_rels/workbook.xml.rels', data: Buffer.from(workbookRels, 'utf8') },
    { name: 'xl/styles.xml', data: Buffer.from(styles, 'utf8') },
    { name: 'xl/worksheets/sheet1.xml', data: Buffer.from(sheet, 'utf8') }
  ]);
}
function handleExportXlsx(req, res) {
  if (!isHq(req)) return redirect(res, '/login');
  const { header, rows } = memberExportData();
  const buf = buildXlsx(header, rows);
  res.writeHead(200, { 'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename="members.xlsx"', 'Content-Length': buf.length });
  res.end(buf);
}

// ---------- リーダー ----------
function getLeader(token) {
  return store.leaders.find(l => l.token === token && l.status === 'active');
}
const CONSENT_TEXT = `
<div class="card"><h2>情報の取り扱いについて</h2>
<p>この画面は、組織図の作成と会社による会員管理のために使います。</p>
<ul>
<li>集める情報：名前、会員ID、バイナリー上の位置、登録日、紹介者（連絡先は集めません）</li>
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
    // 図の中の空き枠フォームからの登録では、登録した枠のアンカーを戻り先に付け、
    // 登録後も作業していた場所（深い段の枠）がそのまま見えるようにする。
    const anchorPart = String(body0.get('from') || '') === 'chart' && body0.get('parentId') && body0.get('side')
      ? '#' + slotAnchorId(body0.get('parentId'), body0.get('side'))
      : '';
    const key = r.result === 'error' ? 'err' : 'msg';
    return redirect(res, `${basePath}?${focusPart}${lvPart}${key}=${encodeURIComponent(r.message)}${anchorPart}`);
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
  // 表示する段数（?levels=）。2〜10に丸め、指定があればデータより深くてもその段数で表示する（renderChartGrid側で打ち切る）。
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
  const lvMinus = Math.max(2, chart.levels - 1);
  const lvPlus = chart.levels < CHART_MAX_DEPTH ? chart.levels + 1 : chart.levels;
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
  <label>紹介者（必須）<input name="introducer" value="${esc(editMember.introducer || '')}" required maxlength="100"></label>
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
  const body = `${searchCard}<div class="card breakout chart-card"><h2>${esc(leader.name)} さんの組織</h2>
  <p class="small">担当トップ：${esc(root ? root.name + '（' + root.memberId + '）' : leader.rootMemberId)} / ここから下だけが表示・入力できます。</p>
  <p class="small">空いている場所の枠に名前を入力して「登録」を押すと、その場所にそのまま登録できます（登録日には今日の日付が入っています。変える場合は日付を変更してください）。紹介者は必須です。必ず入力してください。下のフォームから親と位置を選んで登録することもできます。</p>
  <p class="small">「表示する段数」の＋を押すと、まだ誰もいない先の段まで図を広げて見られます。先の段の「空き」を押すと、その枝の登録できる空き枠へ移動します。</p>
  <p class="small">図の一番下の段にいる会員の「＋ 下を増やす」を押すと、その人を先頭にして、さらに下の枝を見たり入力したりできます（何度でも繰り返せます）。</p>
  ${focusBanner}
  <div class="chart-bar"><span class="zlabel">表示する段数：</span><span class="zoombox"><a class="zbtn" href="${lvHref(lvMinus)}" aria-label="段数を減らす">−</a><input class="pctin" id="lvIn" type="number" min="2" max="10" step="1" value="${chart.levels}" data-href="${lvHref('__N__')}" aria-label="表示する段数">段<a class="zbtn" href="${lvHref(lvPlus)}" aria-label="段数を増やす">＋</a></span><span class="zlabel">図の大きさ：</span><span class="zoombox"><button type="button" id="zOut" aria-label="縮小">−</button><input class="pctin" id="zPctIn" type="number" min="2" max="200" step="1" value="100" aria-label="図の大きさ（パーセント）">%<button type="button" id="zIn" aria-label="拡大">＋</button><button type="button" id="zFit">等倍</button><button type="button" id="zFitAll">全体表示</button><button type="button" id="zPrint">印刷</button></span><span class="zlabel">横スクロール・ピンチで拡大縮小できます</span></div>
  <div class="chart-wrap" id="chartWrap"><div class="chart-box" id="chartBox"><div class="chart-scale" id="chartScale">${chart.html}</div></div></div></div>
  ${editCard}
  ${deleteCard}
  <div class="card"><h3>会員を追加する</h3>${presetNote}${addMemberForm(basePath + '/add', scope, preset)}</div>
  <script>(function(){var z=1;var scale=document.getElementById('chartScale');var box=document.getElementById('chartBox');var pct=document.getElementById('zPctIn');var wrap=document.getElementById('chartWrap');if(!scale||!box||!pct||!wrap)return;function apply(){scale.style.transform='scale('+z+')';box.style.width=Math.ceil(scale.scrollWidth*z)+'px';box.style.height=Math.ceil(scale.scrollHeight*z)+'px';if(document.activeElement!==pct)pct.value=Math.round(z*100);}function zoomAt(nz,cx,cy){nz=Math.min(2,Math.max(0.02,Math.round(nz*100)/100));if(nz===z)return;var r=wrap.getBoundingClientRect();var fx=(cx===undefined?r.left+r.width/2:cx)-r.left;var fy=(cy===undefined?r.top+r.height/2:cy)-r.top;var ux=(wrap.scrollLeft+fx)/z;var uy=(wrap.scrollTop+fy)/z;z=nz;apply();wrap.scrollLeft=Math.round(ux*z-fx);wrap.scrollTop=Math.round(uy*z-fy);}document.getElementById('zIn').addEventListener('click',function(){zoomAt(z*1.25);});document.getElementById('zOut').addEventListener('click',function(){zoomAt(z/1.25);});document.getElementById('zFit').addEventListener('click',function(){zoomAt(1);});document.getElementById('zFitAll').addEventListener('click',function(){fitAll();});document.getElementById('zPrint').addEventListener('click',function(){window.print();});pct.addEventListener('change',function(){var v=parseFloat(pct.value);if(isNaN(v)){pct.value=Math.round(z*100);return;}zoomAt(Math.min(200,Math.max(2,v))/100);pct.value=Math.round(z*100);});pct.addEventListener('keydown',function(e){if(e.key==='Enter'){e.preventDefault();pct.blur();}});var lvIn=document.getElementById('lvIn');if(lvIn){var goLv=function(){var n=parseInt(lvIn.value,10);if(isNaN(n))return;n=Math.min(10,Math.max(2,n));var t=lvIn.getAttribute('data-href');if(t)location.href=t.replace('__N__',n);};lvIn.addEventListener('change',goLv);lvIn.addEventListener('keydown',function(e){if(e.key==='Enter'){e.preventDefault();goLv();}});}function fitAll(){var w=scale.scrollWidth,h=scale.scrollHeight;var aw=wrap.clientWidth-2,ah=wrap.clientHeight-2;if(w>0&&h>0&&aw>0&&ah>0){z=Math.max(0.02,Math.round(Math.min(1,Math.min(aw/w,ah/h))*1000)/1000);apply();wrap.scrollLeft=0;wrap.scrollTop=0;}}function centerTop(){var w=Math.ceil(scale.scrollWidth*z);var aw=wrap.clientWidth;wrap.scrollLeft=w>aw?Math.round((w-aw)/2):0;wrap.scrollTop=0;}function prepPrint(){var w=scale.scrollWidth;var aw=Math.min(wrap.clientWidth-2,1080);if(w>0&&aw>0){var pz=Math.max(0.02,Math.min(1,aw/w));scale.style.transform='none';scale.style.zoom=pz;box.style.width='auto';box.style.height='auto';}}var savedView=null;window.addEventListener('beforeprint',function(){if(savedView)return;savedView={z:z,sl:wrap.scrollLeft,st:wrap.scrollTop};prepPrint();});window.addEventListener('afterprint',function(){if(!savedView)return;scale.style.zoom='';z=savedView.z;apply();wrap.scrollLeft=savedView.sl;wrap.scrollTop=savedView.st;savedView=null;});var pd=0;function tdist(e){var a=e.touches[0],b=e.touches[1];var dx=a.clientX-b.clientX,dy=a.clientY-b.clientY;return Math.sqrt(dx*dx+dy*dy);}function tmid(e){var a=e.touches[0],b=e.touches[1];return[(a.clientX+b.clientX)/2,(a.clientY+b.clientY)/2];}wrap.addEventListener('touchstart',function(e){if(e.touches.length===2)pd=tdist(e);},{passive:true});wrap.addEventListener('touchmove',function(e){if(e.touches.length===2&&pd>0){e.preventDefault();var d=tdist(e);var m=tmid(e);zoomAt(z*d/pd,m[0],m[1]);pd=d;}},{passive:false});wrap.addEventListener('touchend',function(e){if(e.touches.length<2)pd=0;});wrap.addEventListener('touchcancel',function(){pd=0;});wrap.addEventListener('wheel',function(e){if(e.ctrlKey){e.preventDefault();zoomAt(z*(e.deltaY<0?1.1:0.9),e.clientX,e.clientY);}},{passive:false});apply();var hId=location.hash&&location.hash.length>1?location.hash.slice(1):'';var hEl=hId?document.getElementById(hId):null;if(hEl&&hEl.id!=='chartWrap'&&scale.contains(hEl)){var hr=hEl.getBoundingClientRect();var wr=wrap.getBoundingClientRect();wrap.scrollLeft+=Math.round((hr.left+hr.width/2)-(wr.left+wrap.clientWidth/2));wrap.scrollTop+=Math.round((hr.top+hr.height/2)-(wr.top+wrap.clientHeight/2));}else{centerTop();}})();</script>`;
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
    if (p === '/hq/tree' || p === '/hq/tree/add' || p === '/hq/tree/edit' || p === '/hq/tree/delete') return handleHqTree(req, res, url);
    if (p === '/hq/members' || p === '/hq/members/add' || p === '/hq/members/delete') return handleHqMembers(req, res, url);
    if (p === '/hq/leaders') return handleHqLeaders(req, res, url);
    if (p === '/hq/conflicts') return handleHqConflicts(req, res, url);
    if (p === '/hq/export.csv') return handleExportCsv(req, res);
    if (p === '/hq/export.xlsx') return handleExportXlsx(req, res);
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
