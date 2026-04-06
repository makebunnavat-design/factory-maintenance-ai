// Somr AI UI logic (moved from index.html)

function updatePipelineStage(id, stage, isError = false) {
  setLoadingStage(id, isError ? 'ERROR' : stage);
}

function closePipelineModal() {
}

const chatContainer = document.getElementById('chat-container');
const chatForm = document.getElementById('chat-form');
const userInput = document.getElementById('user-input');
const suggestionsDiv = document.getElementById('suggestions');
const sendButton = document.getElementById('send-btn');

function getElinAvatarSrc() {
  return (typeof window !== 'undefined' && window.SOMR_AVATAR_IMG) ? window.SOMR_AVATAR_IMG : '';
}

let suggestionWords = [];
let suggestionLines = [];
let suggestionProcesses = [];
let suggestionTechs = [];
let suggestionPmTasks = [];
let suggestionShared = [];
let suggestionPairs = [];

// --- Meta Mode State ---
let currentMode = 'normal'; // 'normal' หรือ 'meta'
const loadingStateRegistry = new Map();
const LOADING_DETAIL_ROTATE_MS = 6200;
let isChatRequestInFlight = false;
let currentChatAbortController = null;
let currentChatLoadingId = null;
let modeTransitionTimerId = null;

function createAbortError() {
  const err = new Error('Request aborted');
  err.name = 'AbortError';
  return err;
}

function isAbortError(err) {
  return !!err && err.name === 'AbortError';
}

function waitWithAbort(ms, signal) {
  return new Promise((resolve, reject) => {
    if (!signal) {
      window.setTimeout(resolve, ms);
      return;
    }
    if (signal.aborted) {
      reject(createAbortError());
      return;
    }

    const timerId = window.setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    function onAbort() {
      window.clearTimeout(timerId);
      signal.removeEventListener('abort', onAbort);
      reject(createAbortError());
    }

    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function renderSendButtonState() {
  if (!sendButton) return;
  sendButton.disabled = false;
  sendButton.dataset.state = isChatRequestInFlight ? 'cancel' : 'send';
  sendButton.classList.toggle('send-btn--cancel', isChatRequestInFlight);
  sendButton.classList.toggle('send-btn--busy', isChatRequestInFlight);
  sendButton.setAttribute('title', isChatRequestInFlight ? 'หยุดโหลดคำถามนี้' : 'ส่งคำถาม');
  sendButton.setAttribute('aria-label', isChatRequestInFlight ? 'หยุดโหลดคำถามนี้' : 'ส่งคำถาม');
  sendButton.innerHTML = isChatRequestInFlight
    ? '<span class="send-btn-icon-wrap send-btn-icon-wrap--stop" aria-hidden="true"><span class="send-btn-stop-square"></span></span>'
    : '<span class="send-btn-icon-wrap send-btn-icon-wrap--send" aria-hidden="true"><i class="ph-bold ph-paper-plane-right text-lg"></i></span>';
}

function setChatSubmissionLocked(locked) {
  isChatRequestInFlight = locked;
  if (chatForm) chatForm.setAttribute('aria-busy', locked ? 'true' : 'false');
  renderSendButtonState();
}

function cancelActiveChatRequest() {
  if (!isChatRequestInFlight) return false;

  if (currentChatAbortController && !currentChatAbortController.signal.aborted) {
    currentChatAbortController.abort();
  }
  if (currentChatLoadingId) {
    removeLoading(currentChatLoadingId);
    currentChatLoadingId = null;
  }

  updateHeaderStatus('ready');
  setChatSubmissionLocked(false);
  return true;
}

renderSendButtonState();

function syncModeTheme(mode = currentMode, animate = false) {
  const body = document.body;
  if (!body) return;

  body.dataset.mode = mode;
  body.classList.toggle('meta-mode', mode === 'meta');

  if (modeTransitionTimerId) {
    window.clearTimeout(modeTransitionTimerId);
    modeTransitionTimerId = null;
  }

  if (!animate) {
    body.classList.remove('mode-transitioning');
    return;
  }

  body.classList.add('mode-transitioning');
  modeTransitionTimerId = window.setTimeout(() => {
    body.classList.remove('mode-transitioning');
    modeTransitionTimerId = null;
  }, 520);
}

syncModeTheme(currentMode, false);

function triggerTransientClass(element, className) {
  if (!element) return;
  element.classList.remove(className);
  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => {
      element.classList.add(className);
    });
  });
}

function revealTablePanel(tableId) {
  const tablePanel = document.getElementById(tableId);
  if (!tablePanel) return null;
  tablePanel.classList.remove('hidden');
  triggerTransientClass(tablePanel, 'table-panel--visible');
  return tablePanel;
}

function prepareBotMessageReveal(contentEl, actionsEl, readBtnEl) {
  if (!contentEl) return;
  if (contentEl.dataset.revealPrepared === 'true') return;
  contentEl.dataset.revealPrepared = 'true';

  const fragments = [];
  const visibleChildren = Array.from(contentEl.children).filter((child) => {
    return !child.classList.contains('hidden') && !child.classList.contains('table-panel');
  });

  if (visibleChildren.length === 0) {
    const rawHtml = contentEl.innerHTML.trim();
    if (rawHtml) {
      const wrapper = document.createElement('span');
      wrapper.className = 'response-fragment response-fragment--text response-fragment--inline';
      wrapper.innerHTML = rawHtml;
      contentEl.innerHTML = '';
      contentEl.appendChild(wrapper);
      fragments.push(wrapper);
    }
  } else {
    visibleChildren.forEach((child) => {
      child.classList.add('response-fragment');
      if (child.classList.contains('bot-response-text')) {
        child.classList.add('response-fragment--text');
      }
      fragments.push(child);
    });
  }

  fragments.forEach((fragment, index) => {
    fragment.style.setProperty('--response-fragment-index', String(index));
  });

  const nextIndex = fragments.length;
  if (actionsEl && actionsEl.querySelector('button')) {
    actionsEl.classList.add('response-fragment', 'response-fragment--support');
    actionsEl.style.setProperty('--response-fragment-index', String(nextIndex));
  }

  if (readBtnEl) {
    readBtnEl.classList.add('response-fragment', 'response-fragment--support');
    readBtnEl.style.setProperty('--response-fragment-index', String(nextIndex + (actionsEl && actionsEl.querySelector('button') ? 1 : 0)));
  }

  requestAnimationFrame(() => {
    contentEl.classList.add('msg-content--streaming');
  });
}

const LOADING_STAGE_COPY = {
  normal: {
    PENDING: {
      badge: 'Repair Mode',
      title: 'กำลังรับคำถาม',
      details: [
        'กำลังเตรียมคำถามและเช็กข้อมูลล่าสุดให้ก่อนตอบ',
        'กำลังเปิด workflow ที่เหมาะกับคำถามนี้'
      ]
    },
    ROUTING: {
      badge: 'Repair Mode',
      title: 'กำลังทำความเข้าใจคำถาม',
      details: [
        'กำลังเลือกว่าจะใช้ SQL, Vector หรือ Hybrid',
        'กำลังดูว่าคำถามนี้ควรค้นจากฐานซ่อมหรือ PM'
      ]
    },
    ENTITY_MATCHING: {
      badge: 'Repair Mode',
      title: 'กำลังค้นหา Line / Process ที่เกี่ยวข้อง',
      details: [
        'กำลังจับคู่คำที่พิมพ์กับชื่อจริงในฐานข้อมูล',
        'กำลังหาไลน์หรือโปรเซสที่ใกล้เคียงที่สุด'
      ]
    },
    MAIN: {
      badge: 'Repair Mode',
      title: 'กำลังดึงข้อมูลจาก pipeline ที่เลือกไว้',
      details: [
        'กำลังค้นหาข้อมูลที่เกี่ยวข้องกับคำถามนี้',
        'กำลังรวมข้อมูลให้พร้อมสำหรับสรุปคำตอบ'
      ]
    },
    SQL_GEN: {
      badge: 'Repair Mode',
      title: 'กำลังสร้างคำสั่งค้นหาข้อมูล',
      details: [
        'กำลังแปลงคำถามให้เป็น SQL ที่ปลอดภัย',
        'กำลังเตรียม query ให้ตรงกับ schema ของฐานข้อมูล'
      ]
    },
    SUMMARY: {
      badge: 'Repair Mode',
      title: 'กำลังสรุปผลให้อ่านง่าย',
      details: [
        'กำลังจัดรูปแบบคำตอบและผลลัพธ์ให้ดูง่าย',
        'กำลังเตรียมส่งคำตอบกลับมาที่หน้าแชต'
      ]
    },
    SYNC_FALLBACK: {
      badge: 'Repair Mode fallback',
      title: 'กำลังประมวลผลคำถาม',
      details: [
        'ระบบกำลังตอบผ่านเส้นทางสำรอง อาจใช้เวลานานขึ้นเล็กน้อย',
        'กำลังรอผลลัพธ์ชุดเต็มจาก backend'
      ]
    },
    AI_RETRY: {
      badge: 'Repair Mode Retry',
      title: 'กำลังลองตอบแบบละเอียดขึ้น',
      details: [
        'กำลังให้ระบบประมวลผลคำถามเดิมอีกครั้ง',
        'กำลังตรวจคำตอบให้อธิบายครบขึ้นกว่าเดิม'
      ]
    },
    ERROR: {
      badge: 'Repair Mode Recovery',
      title: 'กำลังจัดการข้อผิดพลาด',
      details: [
        'การเชื่อมต่อสะดุดเล็กน้อย กำลังลองประมวลผลต่อ',
        'ถ้ายังไม่สำเร็จ ระบบจะแจ้งกลับในอีกไม่กี่วินาที'
      ]
    },
    DEFAULT: {
      badge: 'Recovery',
      title: 'กำลังค้นหาข้อมูล',
      details: [
        'กำลังประมวลผลคำถามของพี่',
        'กำลังรวบรวมข้อมูลที่จำเป็นสำหรับคำตอบ'
      ]
    }
  },
  meta: {
    PENDING: {
      badge: 'Meta Mode',
      title: 'กำลังเตรียมค้นหาใน Meta',
      details: [
        'กำลังอ่านคำถามและเปิดคลังความรู้ของ Elin',
        'กำลังเตรียมค้นหาหัวข้อที่ใกล้เคียงที่สุด'
      ]
    },
    ROUTING: {
      badge: 'Meta Mode',
      title: 'กำลังค้นหาหัวข้อความรู้',
      details: [
        'กำลังดูว่าความรู้ชิ้นไหนเกี่ยวข้องกับคำถามนี้',
        'กำลังเลือกข้อมูลที่ใกล้เคียงที่สุดจาก Meta Database'
      ]
    },
    MAIN: {
      badge: 'Meta Mode',
      title: 'กำลังค้นหาความรู้ใน Meta Database',
      details: [
        'กำลังเปิดดูคำตอบและหัวข้อที่คล้ายกัน',
        'กำลังรวบรวมความรู้ที่น่าจะตอบคำถามนี้ได้ดีที่สุด'
      ]
    },
    META_SEARCH: {
      badge: 'Meta Mode',
      title: 'กำลังค้นหาความรู้ใน Meta Database',
      details: [
        'กำลังค้นหาหัวข้อที่ใกล้เคียงกับคำถามนี้',
        'กำลังรวบรวมความรู้ที่เกี่ยวข้องเพื่อสรุปคำตอบ'
      ]
    },
    SUMMARY: {
      badge: 'Meta Mode',
      title: 'กำลังสรุปคำตอบจากความรู้ที่พบ',
      details: [
        'กำลังสรุปเนื้อหาให้เข้าใจง่าย',
        'กำลังจัดคำตอบให้น่าอ่านก่อนส่งกลับมา'
      ]
    },
    META_SAVE: {
      badge: 'Meta Mode',
      title: 'กำลังบันทึกความรู้ใหม่',
      details: [
        'กำลังจัดเก็บหัวข้อและคำตอบเข้าสู่ Meta Database',
        'กำลังเตรียมให้ Elin จำความรู้นี้ไว้ใช้งานครั้งถัดไป'
      ]
    },
    META_EMBED: {
      badge: 'Meta Mode',
      title: 'กำลังอัปเดตฐานความรู้',
      details: [
        'กำลัง rebuild embeddings เพื่อให้ค้นหาได้แม่นขึ้น',
        'กำลังปรับดัชนีความรู้ใหม่ให้พร้อมใช้งาน'
      ]
    },
    SYNC_FALLBACK: {
      badge: 'Meta Mode',
      title: 'กำลังค้นหาในโหมดสำรอง',
      details: [
        'กำลังค้นหาความรู้ผ่านเส้นทางสำรองของระบบ',
        'กำลังรอคำตอบจาก Meta Database แบบเต็ม'
      ]
    },
    ERROR: {
      badge: 'Meta Mode',
      title: 'กำลังจัดการข้อผิดพลาด',
      details: [
        'ระบบกำลังพยายามเชื่อมต่อ Meta Database ใหม่',
        'ถ้ายังไม่สำเร็จ Elin จะแจ้งสถานะให้ทันที'
      ]
    },
    DEFAULT: {
      badge: 'Meta Mode',
      title: 'กำลังค้นหาความรู้',
      details: [
        'กำลังค้นหาความรู้ที่เกี่ยวข้องกับคำถามนี้',
        'กำลังรวบรวมข้อมูลเพื่อสรุปคำตอบให้พี่'
      ]
    }
  }
};

function getLoadingMode(mode = currentMode) {
  return mode === 'meta' ? 'meta' : 'normal';
}

function getLoadingCopy(stage, mode = currentMode) {
  const resolvedMode = getLoadingMode(mode);
  const modeCopy = LOADING_STAGE_COPY[resolvedMode] || LOADING_STAGE_COPY.normal;
  return modeCopy[stage] || modeCopy.DEFAULT;
}

function getLoadingAvatar(mode = currentMode) {
  const avatarSrc = getElinAvatarSrc();
  if (avatarSrc) {
    return `<img src="${avatarSrc}" alt="Elin" class="w-full h-full object-cover">`;
  }

  const accentClass = getLoadingMode(mode) === 'meta' ? 'text-blue-400' : 'text-pink-400';
  return `<div class="w-full h-full rounded-full bg-zinc-700 flex items-center justify-center ${accentClass} font-bold text-sm">E</div>`;
}

function renderLoadingState(entry) {
  if (!entry?.element) return;

  const copy = getLoadingCopy(entry.stage, entry.mode);
  const detailList = Array.isArray(copy.details) && copy.details.length ? copy.details : ['กำลังประมวลผลคำถาม'];
  const detailIndex = entry.tick % detailList.length;
  const dots = '.'.repeat((entry.tick % 3) + 1);

  const badgeEl = entry.element.querySelector('[data-loading-badge]');
  const titleEl = entry.element.querySelector('[data-loading-title]');
  const detailEl = entry.element.querySelector('[data-loading-detail]');

  if (badgeEl) badgeEl.textContent = copy.badge || (entry.mode === 'meta' ? 'Meta Mode' : 'กำลังประมวลผล');
  if (titleEl) titleEl.textContent = copy.title || 'กำลังค้นหาข้อมูล';
  if (detailEl) detailEl.textContent = `${detailList[detailIndex]}${dots}`;
}

function animateLoadingStageShift(entry) {
  const bubbleEl = entry?.element?.querySelector('.loading-bubble');
  if (!bubbleEl) return;

  bubbleEl.classList.remove('loading-bubble--stage-shift');
  window.requestAnimationFrame(() => {
    bubbleEl.classList.add('loading-bubble--stage-shift');
  });
}

function setLoadingStage(id, stage, mode) {
  const entry = loadingStateRegistry.get(id);
  if (!entry) return;

  const nextMode = mode ? getLoadingMode(mode) : entry.mode;
  const nextStage = stage || entry.stage;
  const hasChanged = nextMode !== entry.mode || nextStage !== entry.stage;

  entry.mode = nextMode;
  entry.stage = nextStage;

  if (hasChanged) {
    entry.tick = 0;
    renderLoadingState(entry);
    animateLoadingStageShift(entry);
    scrollToBottom();
  }
}

// --- Offline support (cache only; ไม่แสดงแบนเนอร์ออฟไลน์/แคช) ---
const CHAT_CACHE_KEY = 'somr_chat_cache_v1';
const API_CACHE_KEY = 'somr_api_cache_v1';

function _loadJsonCache(key) {
  try { return JSON.parse(localStorage.getItem(key) || '{}'); } catch { return {}; }
}
function _saveJsonCache(key, obj) {
  try { localStorage.setItem(key, JSON.stringify(obj || {})); } catch { }
}
// แบนเนอร์โหมดออฟไลน์/แคช ถูกลบออกตามที่ขอ — ไม่แสดงข้อความ "ใช้ข้อมูลที่แคชไว้ล่าสุด"
function setOfflineBannerVisible(visible, note) {
  // no-op: ไม่แสดงแบนเนอร์ออฟไลน์/แคช
}

// Register Service Worker (cache UI assets + GET /api/*)
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => { });
  });
}

async function safeGetJson(url, cacheKey) {
  // cacheKey optional (defaults to url)
  const key = cacheKey || url;
  try {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error('http_' + res.status);
    const data = await res.json();
    const cache = _loadJsonCache(API_CACHE_KEY);
    cache[key] = { data, saved_at: Date.now() };
    _saveJsonCache(API_CACHE_KEY, cache);
    return { data, fromCache: false };
  } catch (e) {
    const cache = _loadJsonCache(API_CACHE_KEY);
    if (cache[key] && cache[key].data) {
      return { data: cache[key].data, fromCache: true, savedAt: cache[key].saved_at };
    }
    throw e;
  }
}

// Load Suggestions (จับคู่ความหมาย: คำที่อยู่ทั้ง Line และ PM = ใช้ได้ทั้งสองตามคำถาม; คู่ชื่อต่างกัน เช่น PCB-E ↔ PCB LINE E)
safeGetJson('/api/suggestions', 'api:suggestions').then(({ data }) => {
  const d = data || {};
  suggestionWords = d.words || [];
  suggestionLines = d.lines || [];
  suggestionProcesses = d.processes || [];
  suggestionTechs = d.techs || [];
  suggestionPmTasks = d.pm_task_names || [];
  suggestionShared = d.shared || [];
  suggestionPairs = d.line_pm_pairs || [];
}).catch(() => { });

if (sendButton) {
  sendButton.addEventListener('click', (e) => {
    if (!isChatRequestInFlight) return;
    e.preventDefault();
    e.stopPropagation();
    cancelActiveChatRequest();
  });
}

// 1. Send Message Logic
chatForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (isChatRequestInFlight) return;
  const message = userInput.value.trim();
  if (!message) return;
  const requestMode = currentMode;
  const abortController = new AbortController();
  let loadingId = null;
  currentChatAbortController = abortController;
  setChatSubmissionLocked(true);
  try {

  // User Message (Pink Bubble)
  addMessage(message, 'user');
  userInput.value = '';
  suggestionsDiv.classList.add('hidden');
  const hintEl = document.getElementById('suggestions-hint');
  if (hintEl) hintEl.classList.add('hidden');

  // Loading
  loadingId = addLoading({
    mode: requestMode,
    stage: requestMode === 'meta' ? 'META_SEARCH' : 'PENDING'
  });
  currentChatLoadingId = loadingId;

  // 🔄 แสดงสถานะการเช็คข้อมูล
  updateHeaderStatus('checking');

    if (window.location.protocol === 'file:') {
      removeLoading(loadingId);
      addBotMessage('❌ กรุณาเปิดหน้าผ่าน Server เช่น <a href="http://localhost:18080" target="_blank" class="text-pink-400 underline">http://localhost:18080</a> ไม่ต้องเปิดไฟล์ HTML โดยตรงค่ะ', null, null);
      return;
    }
    const apiBase = (window.location.origin || '').replace(/\/$/, '');
    let res;
    
    // 1. ลองส่งคำขอแบบ Async เพื่อเอา Job ID
    try {
      setLoadingStage(loadingId, requestMode === 'meta' ? 'META_SEARCH' : 'ROUTING', requestMode);
      res = await fetch(apiBase + '/chat/async', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, mode: requestMode }),
        signal: abortController.signal
      });
    } catch (netErr) {
      if (abortController.signal.aborted || isAbortError(netErr)) return;
      console.log('Async endpoint failed, trying sync fallback...');
      // Fallback: ใช้ /chat endpoint แบบ synchronous
      try {
        setLoadingStage(loadingId, 'SYNC_FALLBACK', requestMode);
        res = await fetch(apiBase + '/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message, mode: requestMode }),
          signal: abortController.signal
        });
        
        if (res.ok) {
          const data = await res.json();
          removeLoading(loadingId);
          updateHeaderStatus(data.data_updated ? 'updated' : 'ready');
          
          if (data.error) {
            let errorHtml = data.error;
            if (data.error_link_url && data.error_link_text) {
              errorHtml += ' <a href="' + data.error_link_url + '" target="_blank" rel="noopener" class="inline-block mt-2 px-3 py-1.5 bg-pink-600 hover:bg-pink-500 text-white text-sm rounded-lg transition">' + data.error_link_text + '</a>';
            }
            addBotMessage(errorHtml, null, null);
          } else {
            addBotMessage(formatResponse(data, message), data.sql || null, message, data.data || null);
            _saveJsonCache(CHAT_CACHE_KEY, { [message]: { data } });
          }
          return;
        }
      } catch (syncErr) {
        if (abortController.signal.aborted || isAbortError(syncErr)) return;
        console.error('Both async and sync endpoints failed:', syncErr);
      }
      
      // ถ้าทั้ง async และ sync ล้มเหลว ให้ใช้ cache
      removeLoading(loadingId);
      updateHeaderStatus('error');
      const chatCache = _loadJsonCache(CHAT_CACHE_KEY);
      const cached = chatCache[message];
      if (cached && cached.data) {
        const data = cached.data;
        addBotMessage(formatResponse(data, message), data.sql || null, message, data.data || null);
        return;
      }
      addBotMessage('⚠️ ตอนนี้ออฟไลน์/ติดต่อเซิร์ฟเวอร์ไม่ได้ค่ะ แต่ยังเปิดกราฟ/แดชบอร์ดจากข้อมูลที่เคยโหลดได้', null, null);
      return;
    }
    
    if (!res.ok) {
        // ถ้า async endpoint ไม่ทำงาน ให้ลอง sync fallback
        if (res.status === 404 || res.status >= 500) {
          console.log('Async endpoint returned ' + res.status + ', trying sync fallback...');
          try {
            setLoadingStage(loadingId, 'SYNC_FALLBACK', requestMode);
            const syncRes = await fetch(apiBase + '/chat', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ message, mode: requestMode }),
              signal: abortController.signal
            });
            
            if (syncRes.ok) {
              const data = await syncRes.json();
              removeLoading(loadingId);
              updateHeaderStatus(data.data_updated ? 'updated' : 'ready');
              
              if (data.error) {
                let errorHtml = data.error;
                if (data.error_link_url && data.error_link_text) {
                  errorHtml += ' <a href="' + data.error_link_url + '" target="_blank" rel="noopener" class="inline-block mt-2 px-3 py-1.5 bg-pink-600 hover:bg-pink-500 text-white text-sm rounded-lg transition">' + data.error_link_text + '</a>';
                }
                addBotMessage(errorHtml, null, null);
              } else {
                addBotMessage(formatResponse(data, message), data.sql || null, message, data.data || null);
                _saveJsonCache(CHAT_CACHE_KEY, { [message]: { data } });
              }
              return;
            }
          } catch (syncErr) {
            if (abortController.signal.aborted || isAbortError(syncErr)) return;
            console.error('Sync fallback also failed:', syncErr);
          }
        }
        
        removeLoading(loadingId);
        updateHeaderStatus('error');
        addBotMessage('❌ Server ตอบกลับ ' + res.status + ' ค่ะ', null, null);
        return;
    }

    const asyncData = await res.json();
    
    // เช็คว่าถ้าเป็นกรณีพิเศษ (meta_update) Server อาจจะคืนคำตอบมาเลยแบบ Synchronous
    if (asyncData.status === 'success' || asyncData.status === 'error' && asyncData.message) {
        removeLoading(loadingId);
        addBotMessage(asyncData.message, null, null);
        return;
    }
    
    const jobId = asyncData.job_id;
    if (!jobId) {
        removeLoading(loadingId);
        updateHeaderStatus('error');
        addBotMessage('❌ ไม่ได้รับ Job ID จากระบบค่ะ', null, null);
        return;
    }

    // 2. เริ่ม Polling วนลูปเช็คสถานะทุกๆ 30 วินาที
    let data = null;
    let isCompleted = false;
    
    while (!isCompleted) {
        // รอ 2 วินาทีก่อนเช็คครั้งต่อไป (ลดจาก 30 วิเพื่อให้ Pipeline ลื่นไหล)
        await waitWithAbort(2000, abortController.signal);
        
        try {
            const statusRes = await fetch(apiBase + `/chat/status/${jobId}`, { signal: abortController.signal });
            if (!statusRes.ok) {
                if(statusRes.status === 404) {
                    throw new Error("Job expired or not found");
                }
                continue;
            }
            
            const statusData = await statusRes.json();
            
            if (statusData.status === "completed" || statusData.status === "error") {
                data = statusData.result;
                isCompleted = true;
            } else if (statusData.status === "processing") {
                 const activeStage = statusData.stage || (requestMode === 'meta' ? 'META_SEARCH' : 'MAIN');
                 updatePipelineStage(loadingId, activeStage);
                 updateHeaderStatus('checking'); 
             }
        } catch (pollErr) {
            if (abortController.signal.aborted || isAbortError(pollErr)) return;
            console.error("Polling error:", pollErr);
            removeLoading(loadingId);
            updateHeaderStatus('error');
            addBotMessage('❌ เกิดข้อผิดพลาดระหว่างรอคำตอบ: ' + pollErr.message, null, null);
            return;
        }
    }
    
    removeLoading(loadingId);
    
    if (!data) {
        updateHeaderStatus('error');
        addBotMessage('❌ เกิดข้อผิดพลาด ไม่ได้รับข้อมูลคำตอบค่ะ', null, null);
        return;
    }

    updateHeaderStatus(data.data_updated ? 'updated' : 'ready');

    if (data.error) {
      let errorHtml = data.error;
      if (data.error_link_url && data.error_link_text) {
        errorHtml += ' <a href="' + data.error_link_url + '" target="_blank" rel="noopener" class="inline-block mt-2 px-3 py-1.5 bg-pink-600 hover:bg-pink-500 text-white text-sm rounded-lg transition">' + data.error_link_text + '</a>';
      } else {
        errorHtml = '❌ โอ๊ะ! เกิดข้อผิดพลาด: ' + data.error;
      }
      addBotMessage(errorHtml, null, null);
    } else {
      // cache successful chat responses (for offline reuse)
      try {
        const chatCache = _loadJsonCache(CHAT_CACHE_KEY);
        chatCache[message] = { data, saved_at: Date.now() };
        // keep cache small
        const keys = Object.keys(chatCache);
        if (keys.length > 60) {
          // remove oldest
          keys.sort((a, b) => (chatCache[a]?.saved_at || 0) - (chatCache[b]?.saved_at || 0));
          for (let i = 0; i < keys.length - 60; i++) delete chatCache[keys[i]];
        }
        _saveJsonCache(CHAT_CACHE_KEY, chatCache);
      } catch { }
      // 🔄 แสดงสถานะการอัปเดตข้อมูล
      let statusMessage = "";
      if (data.data_updated) {
        statusMessage = `<div class="mb-2 text-xs text-green-400 bg-green-900/20 border border-green-800 rounded p-2">
                            💙 ข้อมูลได้รับการอัปเดตแล้ว (${data.timestamp})
                        </div>`;
      }

      addBotMessage(
        statusMessage + formatResponse(data, message),
        data.sql,
        message,
        data.data // ส่ง raw data สำหรับกราฟ
      );
      refreshTechStatusIfOpen();
    }
  } catch (err) {
    if (abortController.signal.aborted || isAbortError(err)) {
      updateHeaderStatus('ready');
      return;
    }
    removeLoading(loadingId);
    updateHeaderStatus('error');
    const isTimeout = err.name === 'AbortError';
    const msg = isTimeout
      ? '⏱️ คำถามใช้เวลานานเกิน 7 นาที ลองถามสั้นลงหรือลองใหม่อีกครั้งค่ะ'
      : '❌ เชื่อมต่อ Elin ไม่ได้ค่ะ ตรวจสอบว่า Backend รันอยู่ (เช่น uvicorn หรือ Docker) และเปิดผ่าน <a href="http://localhost:18080" class="text-pink-400 underline">http://localhost:18080</a>';
    addBotMessage(msg, null, null);
  } finally {
    if (currentChatAbortController === abortController) currentChatAbortController = null;
    if (currentChatLoadingId === loadingId) currentChatLoadingId = null;
    setChatSubmissionLocked(false);
  }
});

// 2. Render Functions
function addMessage(text, sender) {
  const div = document.createElement('div');
  div.className = `msg-row flex gap-3 ${sender === 'user' ? 'flex-row-reverse' : ''} animate-msg animate-msg--${sender}`;

  // Avatar
  const avatar = sender === 'user'
    ? `<div class="w-9 h-9 rounded-full bg-zinc-700 flex items-center justify-center text-gray-300 text-xs shadow-md flex-shrink-0">You</div>`
    : `<div class="w-10 h-10 rounded-full overflow-hidden bg-zinc-800 border border-zinc-700 shadow-md flex-shrink-0">${getElinAvatarSrc() ? `<img src="${getElinAvatarSrc()}" alt="Elin" class="w-full h-full object-cover">` : '<div class="w-full h-full rounded-full bg-zinc-700 flex items-center justify-center text-pink-400 font-bold text-xl">E</div>'}</div>`;

  // Bubble: ฝั่ง User ใช้ msg-user + animate-msg (animate-msg อยู่ที่แถวแล้ว)
  const bubbleClass = sender === 'user'
    ? 'msg-user'
    : 'msg-bot';

  const now = new Date();
  const timeStr = now.toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit', hour12: false });
  const timeHtml = sender === 'user'
    ? `<div class="text-xs text-zinc-500 mt-1 text-right">${timeStr}</div>`
    : '';

  const bubble = `<div class="${bubbleClass} max-w-[90%] text-sm leading-relaxed">
                ${text}
                ${timeHtml}
            </div>`;

  div.innerHTML = avatar + bubble;
  chatContainer.appendChild(div);
  scrollToBottom();
}

function addBotMessage(htmlContent, sql, questionText, rawData = null) {
  const div = document.createElement('div');
  div.className = "msg-row flex gap-3 items-start animate-msg animate-msg--bot";
  let chartButtonHtml = '';
  const isMetaMode = sql && (sql === 'META_MODE_LLM' || sql === 'META_MODE_NO_DATA' || sql === 'META_MODE');

  if (!isMetaMode && rawData && hasNumericData(rawData)) {
    const chartDataEnc = encodeURIComponent(JSON.stringify(rawData));
    chartButtonHtml = `<div class="msg-bot-actions msg-bot-actions--secondary"><button type="button" onclick="showChart(this.getAttribute('data-chart-data'))" data-chart-data="${chartDataEnc.replace(/"/g, '&quot;')}" class="msg-bot-chart-btn">💗 ดูกราฟ</button></div>`;
  }

  // ปุ่มตอบอีกครั้ง - ถูกปิดการใช้งานตามคำขอของผู้ใช้
  let ai100ButtonHtml = '';

  div.innerHTML = `
                <div class="w-10 h-10 rounded-full overflow-hidden bg-zinc-800 border border-zinc-700 shadow-md flex-shrink-0">${getElinAvatarSrc() ? `<img src="${getElinAvatarSrc()}" alt="Elin" class="w-full h-full object-cover">` : '<div class="w-full h-full rounded-full bg-zinc-700 flex items-center justify-center text-pink-400 font-bold text-xl">E</div>'}</div>
                <div class="flex flex-col max-w-[90%]">
                    <div class="msg-bot p-4">
                        <div class="msg-content">${htmlContent}</div>
                        <div class="msg-bot-actions msg-bot-actions--secondary">
                            ${chartButtonHtml}
                        </div>
                        ${ai100ButtonHtml}
                    </div>
                </div>
                <!-- ปุ่ม TTS ย้ายออกมาข้างนอกกล่องข้อความ -->
                <button type="button" onclick="toggleReadAloud(this)" class="msg-bot-read-btn-external" title="อ่านให้ฟัง">
                    🔊
                    <div class="speaker-waves">
                        <div class="wave"></div>
                        <div class="wave"></div>
                        <div class="wave"></div>
                    </div>
                </button>`;

  chatContainer.appendChild(div);
  prepareBotMessageReveal(
    div.querySelector('.msg-content'),
    div.querySelector('.msg-bot-actions'),
    div.querySelector('.msg-bot-read-btn-external')
  );
  scrollToBottom();
}

/** กดปุ่ม "ตอบอีกครั้ง" → ส่งคำถามเดิมซ้ำด้วย ai_100: true */
async function retryWithAi100(btn) {
  const b64 = btn.getAttribute('data-question-b64');
  if (!b64) return;
  const requestMode = currentMode;
  let question;
  try {
    question = decodeURIComponent(escape(atob(b64)));
  } catch (e) {
    return;
  }
  btn.disabled = true;
  const loadingId = addLoading({
    mode: requestMode,
    stage: requestMode === 'meta' ? 'META_SEARCH' : 'AI_RETRY'
  });
  updateHeaderStatus('checking');
  const apiBase = (window.location.origin || '').replace(/\/$/, '');
  try {
    const res = await fetch(apiBase + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: question, ai_100: true, mode: requestMode })
    });
    const text = await res.text();
    let data = {};
    try { data = JSON.parse(text); } catch (_) { }
    removeLoading(loadingId);
    updateHeaderStatus(data.data_updated ? 'updated' : 'ready');
    if (!res.ok) {
      addBotMessage('❌ Server ตอบกลับ ' + res.status + (data.error ? ': ' + data.error : '') + ' ค่ะ', null, null);
      return;
    }
    if (data.error) {
      let errorHtml = data.error;
      if (data.error_link_url && data.error_link_text) {
        errorHtml += ' <a href="' + data.error_link_url + '" target="_blank" rel="noopener" class="inline-block mt-2 px-3 py-1.5 bg-pink-600 hover:bg-pink-500 text-white text-sm rounded-lg transition">' + data.error_link_text + '</a>';
      }
      addBotMessage(errorHtml, null, null);
    } else {
      let statusMessage = '';
      if (data.data_updated) {
        statusMessage = `<div class="mb-2 text-xs text-green-400 bg-green-900/20 border border-green-800 rounded p-2">💙 ข้อมูลได้รับการอัปเดตแล้ว (${data.timestamp || ''})</div>`;
      }
      addBotMessage(statusMessage + formatResponse(data, question), data.sql, question, data.data);
      refreshTechStatusIfOpen();
    }
  } catch (err) {
    removeLoading(loadingId);
    updateHeaderStatus('error');
    addBotMessage('❌ เชื่อมต่อ Elin ไม่ได้ค่ะ ลองใหม่อีกครั้ง', null, null);
  }
  btn.disabled = false;
}

// 3. Chart Functions
function hasNumericData(data) {
  if (!data || data.length === 0) return false;
  // ตรวจสอบว่ามีคอลัมน์ที่เป็นตัวเลขหรือไม่
  return Object.values(data[0]).some(v => typeof v === 'number' && !isNaN(v));
}

const CHART_LIMIT = 10;

function showChart(encodedData) {
  try {
    if (encodedData == null || encodedData === '') {
      alert('ไม่มีข้อมูลสำหรับแสดงกราฟค่ะ');
      return;
    }
    // ใช้ข้อมูลที่ส่งมา (จากปุ่ม ดูกราฟ — อัปเดตเท่าปัจจุบันเมื่อกด โหลดเพิ่ม หรือ แสดงทั้งหมด)
    let data = JSON.parse(decodeURIComponent(encodedData));
    if (!Array.isArray(data)) data = [];
    if (data.length === 0) { alert('ไม่มีข้อมูลสำหรับแสดงกราฟ'); return; }

    const modal = document.getElementById('chart-modal');
    const chartDom = document.getElementById('dataChart');
    const optionsEl = document.getElementById('chart-options');

    if (typeof echarts === 'undefined') {
      alert('กราฟใช้ ECharts (echarts.min.js) กรุณาโหลดสคริปต์ให้ครบค่ะ');
      return;
    }

    window.chartModalData = data;
    const keys = Object.keys(data[0]);
    const labelKeys = keys.filter(k => typeof data[0][k] === 'string' || typeof data[0][k] !== 'number');
    const valueKeys = keys.filter(k => typeof data[0][k] === 'number');
    if (valueKeys.length === 0) valueKeys.push(keys[1] || keys[0]);
    if (labelKeys.length === 0) labelKeys.push(keys[0]);

    const defaultLabel = labelKeys[0];
    const defaultValue = valueKeys[0];

    function optVal(s) { return String(s).replace(/"/g, '&quot;').replace(/</g, '&lt;'); }
    optionsEl.innerHTML = [
      '<label class="text-zinc-400">แกน X / ป้ายชื่อ</label>',
      '<select id="chart-select-label" class="bg-zinc-700 border border-zinc-600 text-zinc-200 rounded px-2 py-1 text-xs">',
      labelKeys.map(k => '<option value="' + optVal(k) + '"' + (k === defaultLabel ? ' selected' : '') + '>' + escapeHtml(k) + '</option>').join(''),
      '</select>',
      '<label class="text-zinc-400">ค่า (ตัวเลข)</label>',
      '<select id="chart-select-value" class="bg-zinc-700 border border-zinc-600 text-zinc-200 rounded px-2 py-1 text-xs">',
      valueKeys.map(k => '<option value="' + optVal(k) + '"' + (k === defaultValue ? ' selected' : '') + '>' + escapeHtml(k) + '</option>').join(''),
      '</select>',
      '<label class="text-zinc-400">ประเภทกราฟ</label>',
      '<select id="chart-select-type" class="bg-zinc-700 border border-zinc-600 text-zinc-200 rounded px-2 py-1 text-xs">',
      '<option value="bar" selected>แท่ง (Bar)</option>',
      '<option value="line">เส้น (Line)</option>',
      '<option value="pie">วงกลม (Pie)</option>',
      '</select>'
    ].join('');

    optionsEl.querySelector('#chart-select-label').addEventListener('change', applyChartOptions);
    optionsEl.querySelector('#chart-select-value').addEventListener('change', applyChartOptions);
    optionsEl.querySelector('#chart-select-type').addEventListener('change', applyChartOptions);

    modal.classList.remove('hidden');
    triggerTransientClass(modal, 'chart-modal--visible');
    if (window.currentChart && typeof window.currentChart.dispose === 'function') {
      window.currentChart.dispose();
      window.currentChart = null;
    }
    window.currentChart = echarts.init(chartDom);
    triggerTransientClass(chartDom, 'chart-surface--visible');
    applyChartOptions();
    setTimeout(function () {
      if (window.currentChart) window.currentChart.resize();
    }, 100);
  } catch (e) {
    console.error('Chart Error:', e);
    alert('ไม่สามารถสร้างกราฟได้ค่ะ');
  }
}

function applyChartOptions() {
  const data = window.chartModalData;
  if (!data || data.length === 0 || !window.currentChart) return;
  const labelSel = document.getElementById('chart-select-label');
  const valueSel = document.getElementById('chart-select-value');
  const typeSel = document.getElementById('chart-select-type');
  if (!labelSel || !valueSel || !typeSel) return;
  const labelKey = labelSel.value;
  const valueKey = valueSel.value;
  const chartType = typeSel.value;
  const sorted = [...data].sort((a, b) => (Number(b[valueKey]) || 0) - (Number(a[valueKey]) || 0)).slice(0, CHART_LIMIT);
  const option = buildEChartsOptionByChoice(sorted, labelKey, valueKey, chartType);
  window.currentChart.setOption(option, true);
  setTimeout(function () { if (window.currentChart) window.currentChart.resize(); }, 50);
}

function buildChartTooltipFormatter(labelKey, valueKey) {
  return function (params) {
    const p = Array.isArray(params) ? params[0] : params;
    const row = p && p.data && p.data._row;
    if (row && typeof row === 'object') {
      const lines = Object.keys(row).map(k => k + ': ' + (row[k] != null ? row[k] : ''));
      return lines.join('<br/>');
    }
    const name = p && (p.name != null ? p.name : (p.data && p.data.label));
    const val = p && (p.value != null ? p.value : (p.data && p.data.value));
    if (name != null && val != null) return name + '<br/>' + valueKey + ': ' + val;
    return p && p.value != null ? valueKey + ': ' + p.value : '';
  };
}

function buildEChartsOptionByChoice(data, labelKey, valueKey, chartType) {
  const labels = data.map(d => d[labelKey] != null ? String(d[labelKey]) : '');
  const values = data.map(d => Number(d[valueKey]) || 0);
  const colors = ['#ec4899', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#6366f1', '#14b8a6'];
  const tooltipFormatter = buildChartTooltipFormatter(labelKey, valueKey);
  const tooltipBase = {
    trigger: chartType === 'pie' ? 'item' : 'axis',
    backgroundColor: 'rgba(24,24,27,0.95)',
    borderColor: '#3f3f46',
    textStyle: { color: '#e4e4e7', fontSize: 12 },
    formatter: tooltipFormatter
  };
  if (chartType === 'pie') {
    const pieData = labels.map((name, i) => ({ name, value: values[i], itemStyle: { color: colors[i % colors.length] }, _row: data[i] }));
    return {
      title: { text: valueKey, left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 14 } },
      tooltip: tooltipBase,
      legend: { orient: 'vertical', right: 10, top: 'center', textStyle: { color: '#ffffff' } },
      series: [{ type: 'pie', radius: ['40%', '70%'], center: ['50%', '50%'], data: pieData, label: { color: '#ffffff' }, labelLine: { lineStyle: { color: '#ffffff' } } }]
    };
  }
  const pointData = values.map((v, i) => ({ value: v, _row: data[i] }));
  if (chartType === 'line') {
    return {
      title: { text: valueKey, left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 14 } },
      tooltip: tooltipBase,
      xAxis: { type: 'category', data: labels, axisLabel: { color: echartsTheme.axisColor, rotate: 45 }, axisLine: { lineStyle: { color: echartsTheme.gridColor } } },
      yAxis: { type: 'value', axisLabel: { color: echartsTheme.axisColor }, splitLine: { lineStyle: { color: echartsTheme.gridColor } } },
      series: [{ type: 'line', data: pointData, smooth: true, itemStyle: { color: '#ec4899' }, lineStyle: { color: '#ec4899' }, areaStyle: { opacity: 0.2 } }]
    };
  }
  return {
    title: { text: valueKey, left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 14 } },
    tooltip: tooltipBase,
    xAxis: { type: 'category', data: labels, axisLabel: { color: echartsTheme.axisColor, rotate: 45 }, axisLine: { lineStyle: { color: echartsTheme.gridColor } } },
    yAxis: { type: 'value', axisLabel: { color: echartsTheme.axisColor }, splitLine: { lineStyle: { color: echartsTheme.gridColor } } },
    series: [{ type: 'bar', data: pointData, itemStyle: { color: 'rgba(236, 72, 153, 0.8)' }, barMaxWidth: 40 }]
  };
}

var echartsTheme = { textColor: '#e4e4e7', axisColor: '#a1a1aa', gridColor: '#3f3f46' };

function buildEChartsSpeedOption(data) {
  const labels = data.map(d => d.Tech || d.ชื่อ || Object.values(d)[0]);
  const values = data.map(d => d.ResponseMinutes || d.AvgResponseTime || Object.values(d)[1]);
  const colors = values.map(v => (v < 5) ? '#2E8B57' : (v <= 15) ? '#FFD700' : '#DC143C');
  return {
    title: { text: 'Speed Performance', left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 16 } },
    xAxis: { type: 'category', data: labels, axisLabel: { color: echartsTheme.axisColor }, axisLine: { lineStyle: { color: echartsTheme.gridColor } } },
    yAxis: { type: 'value', axisLabel: { color: echartsTheme.axisColor }, splitLine: { lineStyle: { color: echartsTheme.gridColor } } },
    series: [{ type: 'bar', data: values.map((v, i) => ({ value: v, itemStyle: { color: colors[i] } })), barMaxWidth: 40 }]
  };
}

function buildEChartsWorkloadOption(data) {
  const labels = data.map(d => d.Tech || d.ชื่อ || Object.values(d)[0]);
  const values = data.map(d => d.Percentage || d.JobCount || Object.values(d)[1]);
  const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'];
  return {
    title: { text: 'Workload Distribution', left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 16 } },
    tooltip: { trigger: 'item' },
    legend: { orient: 'vertical', right: 10, top: 'center', textStyle: { color: echartsTheme.textColor } },
    series: [{ type: 'pie', radius: ['40%', '70%'], center: ['40%', '50%'], data: labels.map((name, i) => ({ name, value: values[i], itemStyle: { color: colors[i % colors.length] } })) }]
  };
}

function buildEChartsSkillOption(data) {
  const labels = data.map(d => d.Tech || d.ชื่อ || Object.values(d)[0]);
  const values = data.map(d => d.RepairMinutes || d.AvgRepairTime || Object.values(d)[1]);
  const colors = values.map(v => (v <= 30) ? '#2E8B57' : (v <= 60) ? '#FFD700' : '#DC143C');
  return {
    grid: { left: '15%', right: '10%', top: 20, bottom: 20 },
    xAxis: { type: 'value', axisLabel: { color: echartsTheme.axisColor }, splitLine: { lineStyle: { color: echartsTheme.gridColor } } },
    yAxis: { type: 'category', data: labels, axisLabel: { color: echartsTheme.axisColor }, axisLine: { lineStyle: { color: echartsTheme.gridColor } } },
    series: [{ type: 'bar', data: values.map((v, i) => ({ value: v, itemStyle: { color: colors[i] } })), barMaxWidth: 24 }]
  };
}

function buildEChartsDefaultOption(data) {
  const keys = Object.keys(data[0]);
  const labelKey = keys.find(k => typeof data[0][k] === 'string') || keys[0];
  const valueKey = keys.find(k => typeof data[0][k] === 'number') || keys[1];
  const labels = data.map(d => d[labelKey]);
  const values = data.map(d => d[valueKey]);
  return {
    title: { text: valueKey, left: 'center', textStyle: { color: echartsTheme.textColor, fontSize: 14 } },
    xAxis: { type: 'category', data: labels, axisLabel: { color: echartsTheme.axisColor, rotate: 45 }, axisLine: { lineStyle: { color: echartsTheme.gridColor } } },
    yAxis: { type: 'value', axisLabel: { color: echartsTheme.axisColor }, splitLine: { lineStyle: { color: echartsTheme.gridColor } } },
    series: [{ type: 'bar', data: values, itemStyle: { color: 'rgba(236, 72, 153, 0.8)' }, barMaxWidth: 40 }]
  };
}

function closeChart() {
  if (window.currentChart && typeof window.currentChart.dispose === 'function') {
    window.currentChart.dispose();
    window.currentChart = null;
  }
  const modal = document.getElementById('chart-modal');
  const chartDom = document.getElementById('dataChart');
  if (modal) {
    modal.classList.remove('chart-modal--visible');
    modal.classList.add('hidden');
  }
  if (chartDom) chartDom.classList.remove('chart-surface--visible');
}

function escapeHtml(str) {
  if (str == null) return "";
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/**
 * Convert newlines to <br> tags
 */
function nl2br(str) {
  if (str == null) return "";
  return String(str).replace(/\n/g, "<br>");
}

/**
 * Remove duplicated CTA lines that are already represented by action buttons.
 */
function normalizeSummaryText(text) {
  if (text == null) return "";
  return String(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line && !line.includes("ดูรายละเอียดเป็นตาราง"))
    .join("\n")
    .trim();
}

/**
 * แยกข้อความ Description (PM เลื่อน) เป็น oldDate, newDate, reason, reporter
 * รูปแบบ: ย้ายจากวันที่ d-m-yyyy, ถึง d-m-yyyy, เนื่องจาก ..., ผู้แจ้ง ...
 */
function parseDescription(desc) {
  if (!desc) return { oldDate: "", newDate: "", reason: "", reporter: "" };
  const s = String(desc).trim();
  const oldDateMatch = s.match(/ย้ายจากวันที่\s*(\d{1,2}-\d{1,2}-\d{4})/);
  const newDateMatch = s.match(/ถึง\s*(\d{1,2}-\d{1,2}-\d{4})/);
  const reasonMatch = s.match(/เนื่องจาก\s*([\s\S]*?)\s*ผู้แจ้ง/);
  const reporterMatch = s.match(/ผู้แจ้ง\s*(.*)/);
  return {
    oldDate: (oldDateMatch && oldDateMatch[1]) ? oldDateMatch[1] : "",
    newDate: (newDateMatch && newDateMatch[1]) ? newDateMatch[1] : "",
    reason: (reasonMatch && reasonMatch[1]) ? reasonMatch[1].trim() : "",
    reporter: (reporterMatch && reporterMatch[1]) ? reporterMatch[1].trim() : ""
  };
}

/** แปลง Description เป็นข้อความสำหรับแสดง (ใช้ parseDescription ถ้ามี pattern เลื่อน) */
function formatDescriptionCell(desc) {
  if (desc == null || desc === "") return "";
  const p = parseDescription(desc);
  const hasStructured = p.oldDate || p.newDate || p.reason || p.reporter;
  if (hasStructured) {
    const parts = [];
    if (p.oldDate) parts.push("ย้ายจากวันที่ " + p.oldDate);
    if (p.newDate) parts.push("ถึง " + p.newDate);
    if (p.reason) parts.push("เนื่องจาก " + p.reason);
    if (p.reporter) parts.push("ผู้แจ้ง " + p.reporter);
    return parts.join(" | ");
  }
  return String(desc).trim();
}

/** คืนจำนวนรายการขั้นถัดไปสำหรับปุ่มโหลดเพิ่ม (10→30→50→100→200→500) */
function nextLoadMoreLimit(current) {
  const steps = [10, 30, 50, 100, 200, 500];
  for (let i = 0; i < steps.length; i++) {
    if (current < steps[i]) return steps[i];
  }
  return 500;
}

function formatResponse(data, questionText) {
  let html = "";
  // คำตอบแบบถามกลับ (clarification): แสดงเฉพาะข้อความ ไม่แสดง "ไม่พบข้อมูล" หรือ 💖
  if (data.type === 'clarification' && data.text) {
    return `<div class="text-zinc-200 leading-relaxed">${nl2br(escapeHtml(data.text))}</div>`;
  }
  if (data.data && data.data.length > 0) {
    const count = data.row_count != null ? data.row_count : data.data.length;
    const totalCount = data.total_count != null ? data.total_count : (data.row_count != null ? data.row_count : data.data.length);
    const tableId = 'table-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
    const currentShown = data.data.length;
    const canLoadMore = questionText && currentShown > 0 && currentShown < 500 && totalCount > currentShown;
    const summaryText = normalizeSummaryText(data.text);

    // แสดงข้อความสรุปเป็นหลัก (ถ้า Backend ส่งมา)
    if (summaryText) {
      // ใช้ regex หา <br> หรือ <br/> หรือ <br > แบบ case-insensitive
      const hasBrTags = /<br\s*\/?>/i.test(summaryText);

      let formattedText;
      if (hasBrTags) {
        // ถ้ามี <br> อยู่แล้วจาก backend ไม่ต้องใช้ nl2br
        formattedText = summaryText;
      } else {
        // ถ้าไม่มี <br> ให้ใช้ nl2br กับข้อความที่ escape แล้ว
        formattedText = nl2br(escapeHtml(summaryText));
      }

      html += `<div class="bot-response-text">${formattedText}</div>`;
    } else {
      html += `<div class="bot-response-text font-bold text-pink-500 flex items-center gap-2"><span class="bg-pink-500/10 p-1 rounded">🔎</span> เจอ ${count} รายการค่ะ</div>`;
    }

    // เช็คว่าเป็นโหมด Meta หรือไม่
    const isMetaMode = data.sql && (data.sql === 'META_MODE_LLM' || data.sql === 'META_MODE_NO_DATA' || data.sql === 'META_MODE');

    // ปุ่มดูตาราง + โหลดเพิ่ม (ไม่แสดงในโหมด Meta)
    if (!isMetaMode) {
      html += `<div class="msg-bot-actions msg-bot-actions--primary">`;
      html += `<button type="button" onclick="toggleTable('${tableId}')">📋 ดูรายละเอียดเป็นตาราง</button>`;
      if (canLoadMore) {
        const nextLimit = nextLoadMoreLimit(currentShown);
        try {
          const b64 = btoa(unescape(encodeURIComponent(questionText)));
          html += `<button type="button" onclick="loadMoreTable(this)" data-question-b64="${b64}" data-table-id="${tableId}" data-current-limit="${currentShown}" data-row-count="${totalCount}">โหลดเพิ่ม (แสดง ${nextLimit} รายการ)</button>`;
          html += `<button type="button" onclick="loadAllTable(this)" data-question-b64="${b64}" data-table-id="${tableId}" data-current-limit="${currentShown}" data-row-count="${totalCount}">แสดงทั้งหมด</button>`;
        } catch (e) { /* ignore */ }
      }
      html += `</div>`;
    }

    // ตารางซ่อนไว้ — แสดงเมื่อผู้ใช้กดปุ่ม
    html += `<div id="${tableId}" class="hidden mt-3 table-panel" data-table-id="${tableId}">`;
    html += '<div class="table-wrapper custom-scrollbar"><table class="data-table"><thead><tr>';
    const headers = Object.keys(data.data[0]);
    headers.forEach(h => html += `<th>${h}</th>`);
    html += "</tr></thead><tbody>";

    data.data.forEach(row => {
      html += "<tr>";
      headers.forEach(h => {
        const val = row[h];
        const display = (h === "Description" || h === "description") ? formatDescriptionCell(val) : (val != null ? val : "");
        html += `<td>${escapeHtml(display)}</td>`;
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    html += "</div>";
  } else {
    const summaryText = normalizeSummaryText(data.text);
    if (summaryText) {
      // Debug: ดูว่า summaryText มี <br> หรือไม่
      console.log('Meta Mode summaryText:', summaryText);
      // แปลง &lt;br&gt; กลับเป็น <br> (ถ้า backend escape HTML หรือถ้าเป็น meta mode และมี <br> ดั้งเดิม)
      let unescapedText = summaryText
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");

      // ลบ <br> ที่ติดๆ กันเกินความจำเป็น
      unescapedText = unescapedText.replace(/(<br\s*\/?>){3,}/ig, '<br><br>');

      html += `<div class="bot-response-text text-zinc-200">${unescapedText}</div>`;
    } else {
      html += `<div class="text-zinc-500 italic py-2">ไม่พบข้อมูลตามเงื่อนไขเลยค่ะ</div>`;
    }
  }

  // แอบดู SQL (Debug) - ไม่แสดงในโหมด Meta
  const isMetaMode = data.sql && (data.sql === 'META_MODE_LLM' || data.sql === 'META_MODE_NO_DATA' || data.sql === 'META_MODE');
  if (data.sql && !isMetaMode) {
    html += `<details class="msg-bot-debug text-xs text-zinc-500 cursor-pointer transition hover:text-zinc-300">
                    <summary class="py-1 rounded px-2 hover:bg-zinc-800/80">แอบดู SQL (Debug)</summary>
                    <pre class="custom-scrollbar bg-zinc-900 border border-zinc-700 text-green-400 p-3 rounded-lg mt-2 overflow-x-auto font-mono text-[11px]">${escapeHtml(data.sql)}</pre>
                </details>`;
  }

  return html;
}

function toggleTable(tableId) {
  const table = document.getElementById(tableId);
  if (table) {
    const isHidden = table.classList.contains('hidden');
    if (isHidden) {
      revealTablePanel(tableId);
      setTimeout(scrollToBottom, 80);
      setTimeout(scrollToBottom, 250);
    } else {
      table.classList.add('hidden');
      table.classList.remove('table-panel--visible');
    }
  }
}

/** กดปุ่ม "โหลดเพิ่ม" → ส่งคำถามเดิมพร้อม limit_n มากขึ้น แล้วอัปเดตตารางในบล็อกเดิม */
async function loadMoreTable(btn) {
  const b64 = btn.getAttribute('data-question-b64');
  const tableId = btn.getAttribute('data-table-id');
  const currentLimit = parseInt(btn.getAttribute('data-current-limit'), 10) || 10;
  const rowCount = parseInt(btn.getAttribute('data-row-count'), 10) || 0;
  if (!b64 || !tableId) return;
  let question;
  try {
    question = decodeURIComponent(escape(atob(b64)));
  } catch (e) {
    return;
  }
  const nextLimit = nextLoadMoreLimit(currentLimit);
  btn.disabled = true;
  btn.textContent = 'กำลังโหลด...';
  const apiBase = (window.location.origin || '').replace(/\/$/, '');
  try {
    const res = await fetch(apiBase + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: question, limit_n: nextLimit })
    });
    const data = await res.json().catch(() => ({}));
    const container = document.getElementById(tableId);
    if (!container) {
      btn.disabled = false;
      btn.textContent = 'โหลดเพิ่ม (แสดง ' + nextLimit + ' รายการ)';
      return;
    }
    const tbody = container.querySelector('tbody');
    if (data.error || !data.data || !Array.isArray(data.data) || data.data.length === 0 || !tbody) {
      btn.disabled = false;
      btn.textContent = 'โหลดเพิ่ม (แสดง ' + nextLimit + ' รายการ)';
      if (data.error) btn.textContent = 'โหลดไม่สำเร็จ ลองอีกครั้ง';
      return;
    }
    const headers = Object.keys(data.data[0]);
    tbody.innerHTML = data.data.map(row => {
      let tr = '<tr>';
      headers.forEach(h => {
        const val = row[h];
        const display = (h === 'Description' || h === 'description') ? formatDescriptionCell(val) : (val != null ? val : '');
        tr += '<td>' + escapeHtml(display) + '</td>';
      });
      return tr + '</tr>';
    }).join('');
    revealTablePanel(tableId);
    const msgBubble = container.closest('.msg-bot');
    const chartBtn = msgBubble ? msgBubble.querySelector('button[data-chart-data]') : null;
    if (chartBtn && data.data && data.data.length > 0) {
      try {
        const encoded = encodeURIComponent(JSON.stringify(data.data));
        chartBtn.setAttribute('data-chart-data', encoded);
      } catch (e) { /* ignore */ }
    }
    const newShown = data.data.length;
    const totalForButtons = data.total_count != null ? data.total_count : (data.row_count != null ? data.row_count : newShown);
    const canLoadAgain = newShown < Math.min(500, totalForButtons);
    const showAllBtn = msgBubble ? msgBubble.querySelector('button[onclick*="loadAllTable"]') : null;
    if (canLoadAgain) {
      const afterLimit = nextLoadMoreLimit(newShown);
      btn.setAttribute('data-current-limit', String(newShown));
      btn.setAttribute('data-row-count', String(totalForButtons));
      if (showAllBtn) showAllBtn.setAttribute('data-row-count', String(totalForButtons));
      btn.textContent = 'โหลดเพิ่ม (แสดง ' + afterLimit + ' รายการ)';
      btn.disabled = false;
    } else {
      btn.remove();
      if (showAllBtn && (newShown >= totalForButtons || newShown >= 500)) {
        showAllBtn.remove();
      }
    }
  } catch (err) {
    btn.disabled = false;
    btn.textContent = 'โหลดเพิ่ม (แสดง ' + nextLimit + ' รายการ)';
  }
}

/** กดปุ่ม "แสดงทั้งหมด" → โหลดข้อมูลสูงสุด (ไม่เกิน 500) แล้วอัปเดตตารางและปุ่มดูกราฟ */
async function loadAllTable(btn) {
  const b64 = btn.getAttribute('data-question-b64');
  const tableId = btn.getAttribute('data-table-id');
  const rowCount = parseInt(btn.getAttribute('data-row-count'), 10) || 0;
  if (!b64 || !tableId) return;
  let question;
  try {
    question = decodeURIComponent(escape(atob(b64)));
  } catch (e) {
    return;
  }
  const limitAll = Math.min(500, rowCount || 500);
  btn.disabled = true;
  btn.textContent = 'กำลังโหลด...';
  const apiBase = (window.location.origin || '').replace(/\/$/, '');
  try {
    const res = await fetch(apiBase + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: question, limit_n: limitAll })
    });
    const data = await res.json().catch(() => ({}));
    const container = document.getElementById(tableId);
    if (!container) {
      btn.disabled = false;
      btn.textContent = 'แสดงทั้งหมด';
      return;
    }
    const tbody = container.querySelector('tbody');
    if (data.error || !data.data || !Array.isArray(data.data) || data.data.length === 0 || !tbody) {
      btn.disabled = false;
      btn.textContent = 'แสดงทั้งหมด';
      if (data.error) btn.textContent = 'โหลดไม่สำเร็จ';
      return;
    }
    const headers = Object.keys(data.data[0]);
    tbody.innerHTML = data.data.map(row => {
      let tr = '<tr>';
      headers.forEach(h => {
        const val = row[h];
        const display = (h === 'Description' || h === 'description') ? formatDescriptionCell(val) : (val != null ? val : '');
        tr += '<td>' + escapeHtml(display) + '</td>';
      });
      return tr + '</tr>';
    }).join('');
    revealTablePanel(tableId);
    const msgBubble = container.closest('.msg-bot');
    const chartBtn = msgBubble ? msgBubble.querySelector('button[data-chart-data]') : null;
    if (chartBtn && data.data && data.data.length > 0) {
      try {
        chartBtn.setAttribute('data-chart-data', encodeURIComponent(JSON.stringify(data.data)));
      } catch (e) { /* ignore */ }
    }
    const loadMoreBtn = msgBubble ? msgBubble.querySelector('button[onclick*="loadMoreTable"]') : null;
    if (loadMoreBtn && loadMoreBtn.parentElement) loadMoreBtn.remove();
    if (btn.parentElement) btn.remove();
  } catch (err) {
    btn.disabled = false;
    btn.textContent = 'แสดงทั้งหมด';
  }
}

function addLoading(options = {}) {
  const mode = getLoadingMode(options.mode);
  const stage = options.stage || 'PENDING';
  const id = 'loading-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
  const div = document.createElement('div');
  div.id = id;
  const avatarWrapClass = mode === 'meta'
    ? 'loading-avatar-shell loading-avatar-shell--meta w-10 h-10 rounded-full overflow-hidden bg-slate-800 border border-blue-500/40 shadow-[0_0_15px_rgba(59,130,246,0.18)] flex-shrink-0'
    : 'loading-avatar-shell w-10 h-10 rounded-full overflow-hidden bg-zinc-800 border border-zinc-700 flex-shrink-0';
  const bubbleClass = mode === 'meta' ? 'loading-bubble loading-bubble--meta' : 'loading-bubble';
  const loaderClass = mode === 'meta' ? 'triangle-loader triangle-loader--meta' : 'triangle-loader';
  div.className = "loading-row msg-row flex gap-3 mb-4 items-start animate-msg animate-msg--bot";
  div.innerHTML = `
                <div class="${avatarWrapClass}">
                    <span class="loading-avatar-aura" aria-hidden="true"></span>
                    <div class="loading-avatar-core">
                        ${getLoadingAvatar(mode)}
                    </div>
                </div>
                <div class="${bubbleClass}">
                    <div class="loading-progress-line" aria-hidden="true">
                        <span class="loading-progress-bar"></span>
                    </div>
                    <div class="loading-stage-head">
                        <span class="loading-stage-badge" data-loading-badge></span>
                        <div class="loading-stage-title" data-loading-title></div>
                        <div class="loading-stage-detail" data-loading-detail></div>
                    </div>
                    <div class="loading-typing-pulse" aria-hidden="true">
                        <span class="typing-dot"></span>
                        <span class="typing-dot"></span>
                        <span class="typing-dot"></span>
                    </div>
                    <div class="loading-stage-spinner">
                        <div class="${loaderClass}"></div>
                    </div>
                </div>`;
  chatContainer.appendChild(div);

  const entry = {
    id,
    mode,
    stage,
    tick: 0,
    element: div,
    timerId: null
  };

  renderLoadingState(entry);
  animateLoadingStageShift(entry);
  entry.timerId = window.setInterval(() => {
    const current = loadingStateRegistry.get(id);
    if (!current) return;
    current.tick += 1;
    renderLoadingState(current);
  }, LOADING_DETAIL_ROTATE_MS);
  loadingStateRegistry.set(id, entry);

  scrollToBottom();
  return id;
}



function removeLoading(id) {
  const entry = loadingStateRegistry.get(id);
  if (entry?.timerId) {
    window.clearInterval(entry.timerId);
  }
  loadingStateRegistry.delete(id);
  const el = document.getElementById(id);
  if (el) el.remove();
}

function scrollToBottom() {
  if (!chatContainer) return;
  var run = function () {
    chatContainer.scrollTop = chatContainer.scrollHeight;
  };
  run();
  requestAnimationFrame(function () {
    run();
    requestAnimationFrame(run);
  });
  setTimeout(run, 120);
  setTimeout(run, 350);
}

// --- Real-time Data Management ---
function updateHeaderStatus(status) {
  const reloadBtn = document.querySelector('button[onclick="reloadData()"]');
  const statusIndicator = document.querySelector('.status-indicator') || createStatusIndicator();

  switch (status) {
    case 'checking':
      statusIndicator.innerHTML = '💙';
      statusIndicator.className = 'status-indicator text-yellow-400 animate-spin';
      statusIndicator.title = 'กำลังตรวจสอบข้อมูล...';
      break;
    case 'updated':
      statusIndicator.innerHTML = '✅';
      statusIndicator.className = 'status-indicator text-green-400';
      statusIndicator.title = 'ข้อมูลอัปเดตแล้ว';
      setTimeout(() => updateHeaderStatus('ready'), 3000);
      break;
    case 'ready':
      statusIndicator.innerHTML = '💖';
      statusIndicator.className = 'status-indicator text-pink-400';
      statusIndicator.title = 'พร้อมใช้งาน';
      break;
    case 'error':
      statusIndicator.innerHTML = '❌';
      statusIndicator.className = 'status-indicator text-red-400';
      statusIndicator.title = 'เกิดข้อผิดพลาด';
      setTimeout(() => updateHeaderStatus('ready'), 5000);
      break;
  }
}

function createStatusIndicator() {
  const indicator = document.createElement('span');
  indicator.className = 'status-indicator text-pink-400';
  indicator.innerHTML = '💖';
  indicator.title = 'พร้อมใช้งาน';
  const container = document.querySelector('aside p.text-zinc-400') || document.querySelector('.status-indicator')?.parentElement || document.body;
  if (container && container !== document.body) {
    container.appendChild(indicator);
  } else {
    document.body.insertBefore(indicator, document.body.firstChild);
  }
  return indicator;
}

async function reloadData() {
  const reloadBtn = document.querySelector('button[onclick="reloadData()"]');
  const originalText = reloadBtn.innerHTML;

  try {
    // แสดง loading
    reloadBtn.innerHTML = '⏳ <span class="hidden sm:inline">Loading...</span>';
    reloadBtn.disabled = true;

    const response = await fetch('/api/reload', { method: 'POST' });
    const result = await response.json();

    if (result.status === 'success') {
      reloadBtn.innerHTML = '✅ <span class="hidden sm:inline">Updated!</span>';
      reloadBtn.classList.remove('text-green-400');
      reloadBtn.classList.add('text-green-300');
      addBotMessage(`💙 ${result.message}<br><small class="text-zinc-500">เวลา: ${result.timestamp}</small>`, null, null);
      refreshTechStatusIfOpen();
      // รีเซ็ตปุ่มหลัง 3 วินาที
      setTimeout(() => {
        reloadBtn.innerHTML = originalText;
        reloadBtn.classList.remove('text-green-300');
        reloadBtn.classList.add('text-green-400');
        reloadBtn.disabled = false;
      }, 3000);
    } else {
      throw new Error(result.message);
    }

  } catch (error) {
    console.error('Reload error:', error);
    reloadBtn.innerHTML = '❌ <span class="hidden sm:inline">Error</span>';
    reloadBtn.classList.remove('text-green-400');
    reloadBtn.classList.add('text-red-400');

    addBotMessage(`❌ ไม่สามารถอัปเดตข้อมูลได้: ${error.message}`, null, null);

    // รีเซ็ตปุ่มหลัง 3 วินาที
    setTimeout(() => {
      reloadBtn.innerHTML = originalText;
      reloadBtn.classList.remove('text-red-400');
      reloadBtn.classList.add('text-green-400');
      reloadBtn.disabled = false;
    }, 3000);
  }
}

async function checkDataStatus() {
  try {
    const { data: status } = await safeGetJson('/api/data-status', 'api:data-status');

    // แสดงสถานะใน console สำหรับ debug
    console.log('📊 Data Status:', status);

    // ถ้าข้อมูลต้องอัปเดต แสดง notification
    if (status.needs_update) {
      const reloadBtn = document.querySelector('button[onclick="reloadData()"]');
      reloadBtn.classList.add('animate-pulse', 'bg-yellow-600');
      reloadBtn.title = 'มีข้อมูลใหม่! คลิกเพื่ออัปเดต';
    }

    return status;
  } catch (error) {
    console.error('Status check error:', error);
    return null;
  }
}

// เช็คสถานะข้อมูลทุก 30 วินาที
setInterval(checkDataStatus, 30000);

// เช็คสถานะครั้งแรกเมื่อโหลดหน้า
window.addEventListener('load', checkDataStatus);

// --- Suggestions (จับคู่ความหมาย: อันไหนมีคู่ = ใช้ได้ทั้งการซ่อมและ PM; คู่ชื่อต่างกัน เช่น PCB-E ↔ PCB LINE E) ---
const sharedSet = () => new Set(suggestionShared);
const pairLineSet = () => new Set(suggestionPairs.map(p => p[0]));
const pairPmSet = () => new Set(suggestionPairs.map(p => p[1]));
function getSuggestionItems() {
  const set = sharedSet();
  const inPairLine = pairLineSet();
  const inPairPm = pairPmSet();
  const byValue = {};
  suggestionWords.forEach(w => { byValue[w] = 'word'; });
  suggestionLines.forEach(w => {
    if (inPairLine.has(w)) return;
    byValue[w] = set.has(w) ? 'both' : 'line';
  });
  suggestionPmTasks.forEach(w => {
    if (inPairPm.has(w)) return;
    byValue[w] = set.has(w) ? 'both' : 'pm';
  });
  const items = Object.entries(byValue).map(([value, type]) => ({ value, type }));
  suggestionPairs.forEach(([lineVal, pmVal]) => {
    items.push({ value: lineVal, valuePm: pmVal, type: 'pair', display: lineVal + ' ↔ ' + pmVal });
  });
  return items;
}
userInput.addEventListener('input', (e) => {
  const val = e.target.value.toLowerCase();
  const lastWord = val.split(/\s+/).pop() || '';

  if (lastWord.length < 1) {
    suggestionsDiv.classList.add('hidden');
    const h = document.getElementById('suggestions-hint');
    if (h) h.classList.add('hidden');
    return;
  }

  const allItems = getSuggestionItems();
  const matches = allItems.filter(s => {
    const v = (s.display || s.value).toLowerCase();
    const a = s.value.toLowerCase();
    const b = (s.valuePm || '').toLowerCase();
    return a.startsWith(lastWord) || b.startsWith(lastWord) || v.includes(lastWord);
  }).slice(0, 10);

  if (matches.length > 0) {
    const hintEl = document.getElementById('suggestions-hint');
    if (hintEl) hintEl.classList.remove('hidden');
    suggestionsDiv.innerHTML = '';
    const labelByType = { line: 'การซ่อม', pm: 'PM', both: '', word: '' };
    matches.forEach((s, index) => {
      const display = s.type === 'pair' ? s.display : (labelByType[s.type] ? `${s.value} (${labelByType[s.type]})` : s.value);
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'suggestion-btn bg-zinc-800 hover:bg-pink-600 border border-zinc-700 hover:border-pink-500 text-gray-300 hover:text-white px-3 py-1.5 rounded-md whitespace-nowrap transition text-xs';
      btn.style.setProperty('--suggestion-index', String(index));
      btn.dataset.kind = s.type;
      if (s.type === 'line') btn.classList.add('border-amber-500/50', 'hover:border-amber-400');
      if (s.type === 'pm') btn.classList.add('border-pink-500/50', 'hover:border-pink-400');
      if (s.type === 'pair') btn.classList.add('border-cyan-500/50', 'hover:border-cyan-400');
      if (s.type === 'both') btn.title = 'ใช้ได้ทั้งคำถามการซ่อมและ PM (ระบบเข้าใจจากคำถาม)';
      if (s.type === 'pair') btn.title = 'ความหมายเดียวกัน: การซ่อมใช้ค่าแรก, PM ใช้ค่าหลัง (ระบบเลือกให้ตามคำถาม)';
      btn.textContent = display;
      if (s.type === 'line') btn.title = 'ใช้กับคำถามเรื่องการซ่อม/เครื่อง';
      if (s.type === 'pm') btn.title = 'ใช้กับคำถามเรื่อง PM/เลื่อน/Due date';
      btn.addEventListener('click', () => insertWord(s.value));
      suggestionsDiv.appendChild(btn);
    });
    suggestionsDiv.classList.remove('hidden');
  } else {
    const hintEl = document.getElementById('suggestions-hint');
    if (hintEl) hintEl.classList.add('hidden');
    suggestionsDiv.classList.add('hidden');
  }
});

userInput.addEventListener('keydown', (e) => {
  if (e.key === 'Tab' && !suggestionsDiv.classList.contains('hidden')) {
    e.preventDefault();
    const firstBtn = suggestionsDiv.querySelector('button');
    if (firstBtn) firstBtn.click();
  }
});

function insertWord(word) {
  const parts = userInput.value.split(' ');
  parts.pop();
  parts.push(word);
  userInput.value = parts.join(' ') + ' ';
  suggestionsDiv.classList.add('hidden');
  userInput.focus();
}

function toggleDashboard() {
  const modal = document.getElementById('dashboard-modal');
  const grid = document.getElementById('machine-grid');

  if (modal.classList.contains('hidden')) {
    modal.classList.remove('hidden');

    // แสดงข้อมูลสถานะก่อน
    grid.innerHTML = '<div class="text-center text-zinc-400">💙 กำลังโหลดข้อมูล...</div>';

    Promise.all([
      safeGetJson('/api/dashboard', 'api:dashboard').then(r => r.data),
      safeGetJson('/api/data-status', 'api:data-status').then(r => r.data)
    ]).then(([dashboardData, statusData]) => {

      // แสดงสถานะข้อมูลด้านบน
      let statusHtml = '<div class="mb-4 p-3 bg-zinc-800 rounded-lg border border-zinc-700">';
      statusHtml += '<h4 class="text-sm font-bold text-pink-400 mb-2">💗 สถานะข้อมูล</h4>';

      if (statusData.total_records) {
        statusHtml += `<div class="text-xs text-zinc-300">`;
        statusHtml += `<div>💙 อัปเดตล่าสุด: <span class="text-white font-bold">${statusData.last_processed}</span></div>`;
        statusHtml += `</div>`;

        if (statusData.needs_update) {
          statusHtml += '<div class="mt-2 text-xs text-yellow-400">⚠️ มีข้อมูลใหม่ที่ยังไม่ได้อัปเดต</div>';
        }
      }
      statusHtml += '</div>';

      if (!dashboardData.lines || Object.keys(dashboardData.lines).length === 0) {
        grid.innerHTML = statusHtml + '<p class="text-center col-span-full text-zinc-500 mt-10">ยังไม่มีข้อมูลซ่อมวันนี้ค่ะ (เครื่องนิ่งกริบ)</p>';
        return;
      }

      let html = statusHtml;

      // วนลูปแสดงแต่ละ Line (Collapsible)
      for (const [lineName, lineData] of Object.entries(dashboardData.lines)) {
        // กำหนดสีของ Line Header ตามสถานะ
        let lineColor = 'bg-green-900/30 border-green-700 text-green-400';
        if (lineData.status === 'warning') lineColor = 'bg-yellow-900/30 border-yellow-700 text-yellow-400';
        if (lineData.status === 'critical') lineColor = 'bg-red-900/40 border-red-700 text-red-400 animate-pulse';

        const lineId = `line-${lineName.replace(/[^a-zA-Z0-9]/g, '')}`;

        html += `
                        <div class="mb-4">
                            <!-- Line Header (Clickable) -->
                            <div class="${lineColor} border-2 rounded-lg p-4 shadow-lg cursor-pointer hover:brightness-110 transition" 
                                 onclick="toggleLineDetails('${lineId}')">
                                <div class="flex justify-between items-center">
                                    <div class="flex items-center gap-3">
                                        <span id="arrow-${lineId}" class="text-lg transition-transform">▶</span>
                                        <h3 class="font-bold text-lg">💜 ${lineName}</h3>
                                    </div>
                                    <div class="text-right">
                                        <div class="text-2xl font-bold">${lineData.total_repairs}</div>
                                        <div class="text-xs opacity-75">รวมซ่อม</div>
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Process Details (Hidden by default) -->
                            <div id="${lineId}" class="hidden mt-3 ml-6 grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 gap-2 transition-all duration-300">
                        `;

        // แสดง Process ย่อยๆ
        lineData.processes.forEach(process => {
          let processColor = 'bg-green-900/20 border-green-800 text-green-300';
          if (process.status === 'warning') processColor = 'bg-yellow-900/20 border-yellow-800 text-yellow-300';
          if (process.status === 'critical') processColor = 'bg-red-900/30 border-red-800 text-red-300';

          html += `
                            <div class="${processColor} border rounded-md p-2 text-center shadow-sm hover:brightness-125 transition cursor-pointer">
                                <div class="font-semibold text-xs truncate mb-1" title="${process.name}">${process.name}</div>
                                <div class="text-lg font-bold">${process.count}</div>
                                <div class="text-[8px] opacity-75">ครั้ง</div>
                            </div>`;
        });

        html += `
                            </div>
                        </div>`;
      }

      grid.innerHTML = html;

      // แสดงวันที่ข้อมูล
      if (dashboardData.date) {
        const dateInfo = document.createElement('div');
        dateInfo.className = 'text-center text-xs text-zinc-500 mt-4 border-t border-zinc-800 pt-4';
        dateInfo.innerHTML = `💛 ข้อมูล ณ วันที่: ${dashboardData.date}`;
        grid.appendChild(dateInfo);
      }
    }).catch(error => {
      console.error('Dashboard error:', error);
      grid.innerHTML = '<div class="text-center text-red-400">❌ ไม่สามารถโหลดข้อมูลได้</div>';
    });
  } else {
    modal.classList.add('hidden');
  }
}

function toggleLineDetails(lineId) {
  const detailsDiv = document.getElementById(lineId);
  const arrow = document.getElementById(`arrow-${lineId}`);

  if (detailsDiv.classList.contains('hidden')) {
    // ขยาย
    detailsDiv.classList.remove('hidden');
    arrow.style.transform = 'rotate(90deg)';
    arrow.innerHTML = '▼';
  } else {
    // ย่อ
    detailsDiv.classList.add('hidden');
    arrow.style.transform = 'rotate(0deg)';
    arrow.innerHTML = '▶';
  }
}

function renderTechStatusContent(techData, statusData) {
  let html = '';
  if (statusData && statusData.total_records) {
    html += '<div class="mb-6 p-4 bg-zinc-800 rounded-lg border border-zinc-700">';
    html += '<h4 class="text-sm font-bold text-pink-400 mb-3">💗 สถานะข้อมูล</h4>';
    html += '<div class="text-xs text-zinc-300">';
    html += '<div class="text-center"><div class="text-white font-bold text-lg">' + (statusData.last_processed || 'N/A') + '</div><div>อัปเดตล่าสุด</div></div>';
    html += '</div>';
    if (statusData.needs_update) {
      html += '<div class="mt-3 text-xs text-yellow-400 text-center">⚠️ มีข้อมูลใหม่ที่ยังไม่ได้อัปเดต</div>';
    }
    html += '</div>';
  }
  if (techData && techData.teams && Object.keys(techData.teams).length > 0) {
    html += '<div class="mb-6">';
    html += '<h4 class="text-lg font-bold text-blue-400 mb-4">👥 สถานะทีมงาน</h4>';

    Object.entries(techData.teams).forEach(([teamName, teamData], index) => {
      const teamColor = index === 0 ? 'green' : index === 1 ? 'blue' : 'purple';
      const teamId = `team-${teamName.replace(/[^a-zA-Z0-9]/g, '')}`;

      html += `
                        <div class="mb-4">
                            <div class="bg-${teamColor}-900/30 border-2 border-${teamColor}-700 rounded-lg p-4 shadow-lg cursor-pointer hover:brightness-110 transition" 
                                 onclick="toggleTeamDetails('${teamId}')">
                                <div class="flex justify-between items-center">
                                    <div class="flex items-center gap-3">
                                        <span id="arrow-${teamId}" class="text-lg transition-transform">▶</span>
                                        <h5 class="font-bold text-${teamColor}-400 text-lg">${teamName}</h5>
                                    </div>
                                    <div class="text-right">
                                        <div class="text-xs opacity-75">สถิติในวันนี้</div>
                                    </div>
                                </div>
                                <div class="mt-2 grid grid-cols-3 gap-4 text-sm">
                                    <div class="text-center">
                                        <div class="text-yellow-400 font-bold">${teamData.mvp_most_jobs || 'ไม่มีข้อมูล'}</div>
                                        <div class="text-xs opacity-75">ซ่อมจำนวนครั้งมากที่สุด</div>
                                    </div>
                                    <div class="text-center">
                                        <div class="text-yellow-400 font-bold">${teamData.mvp_most_repair_minutes || 'ไม่มีข้อมูล'}</div>
                                        <div class="text-xs opacity-75">เวลาที่ซ่อมมากที่สุด</div>
                                    </div>
                                    <div class="text-center">
                                        <div class="text-yellow-400 font-bold">${teamData.mvp_fastest_response || 'ไม่มีข้อมูล'}</div>
                                        <div class="text-xs opacity-75">โดนเรียกแล้วไปเร็วที่สุด</div>
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Tech Details (Hidden by default) -->
                            <div id="${teamId}" class="hidden mt-3 ml-6 space-y-3 transition-all duration-300">`;

      if (teamData.technicians && teamData.technicians.length > 0) {
        teamData.technicians.forEach((t) => {
          const techName = t.name || t;
          html += `
                                <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-3 hover:bg-zinc-750 transition cursor-pointer flex items-center justify-between gap-3" 
                                     onclick="showTechnicianDetail('${techName}', '${teamName}', ${JSON.stringify(t).replace(/"/g, '&quot;')})">
                                    <div class="font-bold text-white">${techName}</div>
                                    <span class="text-xs text-zinc-500">คลิกดูรายละเอียด →</span>
                                </div>`;
        });
      } else {
        html += '<div class="text-center text-zinc-400 p-4">ไม่มีข้อมูลช่างในทีมนี้</div>';
      }

      html += `
                            </div>
                        </div>`;
    });

    html += '</div>';
  } else {
    html += `<div class="text-center text-zinc-500 p-8">
                    <div class="mb-4">ไม่มีข้อมูลทีมงาน</div>
                    <div class="text-xs">Debug: ${JSON.stringify(techData || {})}</div>
                </div>`;
  }
  return html;
}

function fetchTechStatusData() {
  const apiBase = (window.location.origin || '').replace(/\/$/, '');
  return Promise.all([
    fetch(apiBase + '/api/tech-dashboard', { cache: 'no-store' }).then(r => r.ok ? r.json() : {}).catch(() => ({})),
    safeGetJson('/api/data-status', 'api:data-status').then(r => r.data).catch(() => ({}))
  ]).then(([techResp, statusData]) => {
    const techData = (techResp && techResp.teams) ? techResp : { teams: {} };
    return [techData, statusData];
  });
}

function refreshTechStatusIfOpen() {
  const modal = document.getElementById('tech-status-modal');
  const content = document.getElementById('tech-status-content');
  if (!modal || modal.classList.contains('hidden')) return;
  fetchTechStatusData().then(([techData, statusData]) => {
    content.innerHTML = renderTechStatusContent(techData, statusData);
  }).catch(() => { });
}

function openTechStatus() {
  const modal = document.getElementById('tech-status-modal');
  const content = document.getElementById('tech-status-content');
  modal.classList.remove('hidden');
  content.innerHTML = '<div class="text-center text-zinc-400">💙 กำลังโหลดข้อมูล...</div>';
  if (window._techStatusRefreshInterval) clearInterval(window._techStatusRefreshInterval);
  fetchTechStatusData().then(([techData, statusData]) => {
    content.innerHTML = renderTechStatusContent(techData, statusData);
    window._techStatusRefreshInterval = setInterval(refreshTechStatusIfOpen, 60000);
  }).catch(error => {
    console.error('Tech Status error:', error);
    content.innerHTML = '<div class="text-center text-red-400 p-8">' +
      '<div class="text-4xl mb-4">❌</div>' +
      '<div class="text-lg font-bold mb-2">ไม่สามารถโหลดข้อมูลได้</div>' +
      '<div class="text-sm text-zinc-500">กรุณาตรวจสอบการเชื่อมต่อ API</div>' +
      '</div>';
  });
}

function toggleTeamDetails(teamId) {
  const detailsDiv = document.getElementById(teamId);
  const arrow = document.getElementById(`arrow-${teamId}`);

  if (detailsDiv.classList.contains('hidden')) {
    detailsDiv.classList.remove('hidden');
    arrow.style.transform = 'rotate(90deg)';
    arrow.innerHTML = '▼';
  } else {
    detailsDiv.classList.add('hidden');
    arrow.style.transform = 'rotate(0deg)';
    arrow.innerHTML = '▶';
  }
}

// --- Technician detail modal (drill-down) ---
function showTechnicianDetail(techName, teamName, techData, performanceData) {
  // เรียก API เพื่อดึงข้อมูลรายละเอียดจริง
  fetch(`/api/tech-detail/${encodeURIComponent(techName)}?team_name=${encodeURIComponent(teamName)}`)
    .then(response => response.json())
    .then(data => {
      if (data.error) {
        console.error('Tech detail error:', data.error);
        // ใช้ข้อมูล mock แทน
        showTechnicianDetailModal(techName, teamName, techData, performanceData, null);
        return;
      }
      // ใช้ข้อมูลจริงจาก API
      showTechnicianDetailModal(techName, teamName, techData, performanceData, data);
    })
    .catch(error => {
      console.error('API call failed:', error);
      // ใช้ข้อมูล mock แทน
      showTechnicianDetailModal(techName, teamName, techData, performanceData, null);
    });
}

function showTechnicianDetailModal(techName, teamName, techData, performanceData, realData) {
  // สร้าง modal สำหรับแสดงรายละเอียดช่าง
  const modal = document.createElement('div');
  modal.className = 'fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4';
  modal.id = 'technician-detail-modal';

  const totalJobs = realData ? realData.summary.total_jobs : (techData && (techData.job_count ?? techData.JobCount));
  const jobsToday = realData && realData.summary.jobs_today != null ? realData.summary.jobs_today : 0;
  const repairMinutesToday = realData && realData.summary.repair_minutes_today != null ? realData.summary.repair_minutes_today : 0;
  const totalRepairMinutes = realData && realData.summary.total_repair_minutes != null
    ? realData.summary.total_repair_minutes
    : (totalJobs && (techData && techData.avg_repair) ? Math.round(totalJobs * (techData.avg_repair || 30)) : 0);
  const jobsPerMonth = realData && realData.summary.jobs_per_month != null ? realData.summary.jobs_per_month : totalJobs;
  const repairMinutesPerMonth = realData && realData.summary.repair_minutes_per_month != null ? realData.summary.repair_minutes_per_month : totalRepairMinutes;
  const avgMinutesPerRepairMonth = realData && realData.summary.avg_minutes_per_repair_month != null
    ? realData.summary.avg_minutes_per_repair_month
    : (jobsPerMonth ? Math.round((repairMinutesPerMonth / jobsPerMonth) * 10) / 10 : 0);
  const avgResponseMinutesPerMonth = realData && realData.summary.avg_response_minutes_per_month != null ? realData.summary.avg_response_minutes_per_month : 0;

  const rawTechId = (realData && realData.tech_id) || (techData && techData.tech_id) || '';
  const displayTechId = rawTechId && String(rawTechId).startsWith('REMOVED_') ? '' : rawTechId;

  modal.innerHTML = `
                <div class="bg-zinc-900 border border-zinc-700 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col">
                    <div class="p-4 border-b border-zinc-700 flex justify-between items-center bg-zinc-900">
                        <h3 class="font-bold text-lg text-blue-400">💗 รายละเอียดช่าง - ${techName}</h3>
                        <button onclick="closeTechnicianDetail()" class="text-gray-400 hover:text-white text-2xl transition">&times;</button>
                    </div>
                    <div class="p-6 overflow-y-auto bg-black flex-1">
                        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                            <div class="lg:col-span-1 space-y-4">
                                <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-6 text-center">
                                    <h4 class="text-xl font-bold text-white mb-2">${techName}</h4>
                                    ${displayTechId ? `<div class="text-sm text-blue-400 mb-1">Tech ID: ${displayTechId}</div>` : ''}
                                    <div class="text-sm text-zinc-400 mb-4">${teamName}</div>
                                    <div class="bg-blue-900/30 border border-blue-700 rounded-lg p-4">
                                        <div class="text-3xl font-bold text-blue-400">${totalJobs || 0}</div>
                                        <div class="text-sm text-zinc-400">งานทั้งหมด</div>
                                    </div>
                                </div>
                                <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-4">
                                    <h5 class="font-bold text-white mb-3">💗 สถิติด่วน</h5>
                                    <div class="space-y-2 text-sm">
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">วันนี้ซ่อมกี่ครั้ง:</span>
                                            <span class="text-white">${jobsToday} ครั้ง</span>
                                        </div>
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">ซ่อมวันนี้รวมกันกี่นาที:</span>
                                            <span class="text-pink-400">${typeof repairMinutesToday === 'number' ? Math.round(repairMinutesToday) : repairMinutesToday} นาที</span>
                                        </div>
                                        <div class="border-t border-zinc-600 pt-2 mt-2"></div>
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">ครั้งในการซ่อมต่อเดือน:</span>
                                            <span class="text-white">${jobsPerMonth || 0} ครั้ง</span>
                                        </div>
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">เวลาในการซ่อมต่อเดือนทั้งหมด:</span>
                                            <span class="text-white">${typeof repairMinutesPerMonth === 'number' ? Math.round(repairMinutesPerMonth) : repairMinutesPerMonth} นาที</span>
                                        </div>
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">เฉลี่ยเวลาซ่อมต่อเดือนต่อครั้ง:</span>
                                            <span class="text-pink-400">${avgMinutesPerRepairMonth} นาที</span>
                                        </div>
                                        <div class="border-t border-zinc-600 pt-2 mt-2"></div>
                                        <div class="flex justify-between">
                                            <span class="text-zinc-400">เฉลี่ยเวลาโดนเรียกแล้วไปถึงใช้เวลากี่นาทีต่อเดือน:</span>
                                            <span class="text-pink-400">${avgResponseMinutesPerMonth} นาที</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="lg:col-span-2 grid grid-cols-1 gap-4">
                                <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-4">
                                    <h5 class="font-bold text-yellow-400 mb-3">💜 ปัญหาที่ซ่อม (ย้อนหลัง 30 วัน)</h5>
                                    <div class="space-y-3">
                                        ${(realData && realData.skill_breakdown ? realData.skill_breakdown : []).map(skill => `
                                            <div>
                                                <div class="flex justify-between text-sm mb-1">
                                                    <span class="text-zinc-400">${skill.problem}</span>
                                                    <span class="text-white">${skill.avg_time} นาที</span>
                                                </div>
                                                <div class="w-full bg-zinc-700 rounded-full h-2">
                                                    <div class="bg-yellow-500 h-2 rounded-full" style="width: ${skill.score}%"></div>
                                                </div>
                                            </div>
                                        `).join('') || '<div class="text-zinc-500">ไม่มีข้อมูล</div>'}
                                    </div>
                                </div>
                                <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-4">
                                    <h5 class="font-bold text-purple-400 mb-3">💙 Line ที่ซ่อม</h5>
                                    <div class="grid grid-cols-2 gap-2 text-xs">
                                        ${(realData && realData.versatility_data && realData.versatility_data.length ? realData.versatility_data : []).map((line) => `
                                            <div class="bg-purple-900/30 border border-purple-700 rounded p-2">
                                                <div class="text-purple-400 font-bold">${line.line}</div>
                                                <div class="text-zinc-400">${line.count || 0} งาน</div>
                                            </div>
                                        `).join('') || '<div class="text-zinc-500 p-2">ไม่มีข้อมูล Line</div>'}
                                    </div>
                                </div>
                                <div class="lg:col-span-3 mt-6 w-full">
                                    <h5 class="font-bold text-white mb-3">💛 เวลาซ่อมทั้งหมดต่อ</h5>
                                    <div class="flex gap-2 mb-3">
                                        <button type="button" class="tech-trend-tab px-4 py-2 rounded-lg text-sm font-medium bg-zinc-700 text-zinc-400 hover:bg-zinc-600" data-period="day">วัน</button>
                                        <button type="button" class="tech-trend-tab px-4 py-2 rounded-lg text-sm font-medium bg-blue-600 text-white" data-period="week">สัปดาห์</button>
                                        <button type="button" class="tech-trend-tab px-4 py-2 rounded-lg text-sm font-medium bg-zinc-700 text-zinc-400 hover:bg-zinc-600" data-period="month">เดือน</button>
                                        <button type="button" class="tech-trend-tab px-4 py-2 rounded-lg text-sm font-medium bg-zinc-700 text-zinc-400 hover:bg-zinc-600" data-period="year">ปี</button>
                                    </div>
                                    <div class="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
                                        <canvas id="techDetailTrendChart" height="200"></canvas>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;

  document.body.appendChild(modal);
  modal.dataset.techName = techName;

  // โหลด Repair Time Trend (default: week)
  loadTechTrendChart(techName, 'week');
  const trendTabs = modal.querySelectorAll('.tech-trend-tab');
  trendTabs.forEach(btn => {
    btn.addEventListener('click', function () {
      trendTabs.forEach(b => { b.classList.remove('bg-blue-600', 'text-white'); b.classList.add('bg-zinc-700', 'text-zinc-400'); });
      this.classList.remove('bg-zinc-700', 'text-zinc-400'); this.classList.add('bg-blue-600', 'text-white');
      loadTechTrendChart(modal.dataset.techName, this.dataset.period);
    });
  });
}

function loadTechTrendChart(techName, period) {
  const canvas = document.getElementById('techDetailTrendChart');
  if (!canvas) return;
  fetch('/api/tech-detail/' + encodeURIComponent(techName) + '/trend?period=' + period)
    .then(r => r.json())
    .then(res => {
      const data = res.data || [];
      const labels = data.map(d => d.date);
      const values = data.map(d => d.repair_minutes);
      const ctx = canvas.getContext('2d');
      if (window.techTrendChart) window.techTrendChart.destroy();
      window.techTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: labels,
          datasets: [{
            label: 'Total Repair (นาทีรวม)',
            data: values,
            borderColor: '#ec4899',
            backgroundColor: 'rgba(236, 72, 153, 0.15)',
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#a1a1aa', maxRotation: 45 }, grid: { color: '#3f3f46' } },
            y: { ticks: { color: '#a1a1aa' }, grid: { color: '#3f3f46' }, beginAtZero: true }
          }
        }
      });
    })
    .catch(() => { });
}

function closeTechnicianDetail() {
  if (window.techTrendChart) {
    window.techTrendChart.destroy();
    window.techTrendChart = null;
  }
  const modal = document.getElementById('technician-detail-modal');
  if (modal) {
    modal.remove();
  }
}

function closeTechStatus() {
  if (window._techStatusRefreshInterval) { clearInterval(window._techStatusRefreshInterval); window._techStatusRefreshInterval = null; }
  document.getElementById('tech-status-modal').classList.add('hidden');
}

// Help Guide Functions
function openHelpGuide() {
  const modal = document.getElementById('help-modal');
  const content = document.getElementById('help-content');

  modal.classList.remove('hidden');

  // สร้างเนื้อหาคู่มือ (ยกมาจาก index.html เดิม)
  content.innerHTML = `
            <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-4 mb-4">
                <h4 class="font-bold text-pink-400 mb-2">💖 ยินดีต้อนรับสู่ Elin AI</h4>
                <p class="text-zinc-300">Elin เป็น AI ที่จะช่วยคุณวิเคราะห์ข้อมูลการซ่อมและ PM (Preventive Maintenance) ได้อย่างรวดเร็วและแม่นยำ</p>
                <p class="text-xs text-zinc-400 mt-2">💡 คลิกที่ตัวอย่างคำถามด้านล่างเพื่อลองถามทันที</p>
                <p class="text-xs text-amber-200/90 mt-2">📌 Elin จะตอบบางคำถามส่วนใหญ่แค่ 10 รายการ ถ้าต้องการแสดงมากขึ้น ให้ระบุในคำถาม เช่น <b>20 รายการ</b> หรือ <b>3 อันดับ</b> แล้วกด <b>📋 ดูรายละเอียดเป็นตาราง</b> เพื่อดูตารางเต็ม</p>
            </div>
            <div class="bg-zinc-800 border border-zinc-700 rounded-lg p-4 mb-4">
                <h4 class="font-bold text-yellow-400 mb-3">📋 ตัวอย่างคำถามที่ถามได้</h4>
                <div class="mb-4">
                    <h5 class="font-semibold text-amber-400 mb-2">📌 ประวัติการซ่อม ตาม Line / Process / ช่วงเวลา</h5>
                    <p class="text-xs text-zinc-400 mb-2">Line ทั้งหมด — คลิกเพื่อดูประวัติการซ่อมของ Line นั้น</p>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm max-h-48 overflow-y-auto custom-scrollbar">${(function () {
      const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      const escAttr = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '&quot;');
      return (suggestionLines || []).map(line => {
        const q = 'ประวัติการซ่อมของ ' + line;
        return '<li><button onclick="askExample(\'' + escAttr(q) + '\')" class="text-pink-400 hover:underline">ประวัติการซ่อมของ ' + esc(line) + '</button></li>';
      }).join('');
    })()}</ul>
                    <p class="text-xs text-zinc-400 mt-3 mb-2">Process ทั้งหมด — คลิกเพื่อดูประวัติการซ่อมของ Process นั้น</p>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm max-h-48 overflow-y-auto custom-scrollbar">${(function () {
      const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      const escAttr = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '&quot;');
      return (suggestionProcesses || []).map(proc => {
        const q = 'ประวัติการซ่อมของ ' + proc;
        return '<li><button onclick="askExample(\'' + escAttr(q) + '\')" class="text-pink-400 hover:underline">ประวัติการซ่อมของ ' + esc(proc) + '</button></li>';
      }).join('');
    })()}</ul>
                </div>
                <div class="mb-4">
                    <h5 class="font-semibold text-green-400 mb-2">🔧 การซ่อม - ภาพรวมและสถิติ</h5>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm">
                        <li><button onclick="askExample('วันนี้มีอะไรเสียบ้าง')" class="text-pink-400 hover:underline">วันนี้มีอะไรเสียบ้าง</button> - ดูภาพรวมการเสียวันนี้</li>
                        <li><button onclick="askExample('เมื่อวานมีเครื่องเสียกี่ครั้ง')" class="text-pink-400 hover:underline">เมื่อวานมีเครื่องเสียกี่ครั้ง</button> - นับจำนวนครั้ง</li>
                        <li><button onclick="askExample('สัปดาห์นี้เครื่องเสียอะไรบ้าง')" class="text-pink-400 hover:underline">สัปดาห์นี้เครื่องเสียอะไรบ้าง</button> - ภาพรวมสัปดาห์</li>
                        <li><button onclick="askExample('สัปดาห์นี้กะดึกมีอะไรเสียบ้าง')" class="text-pink-400 hover:underline">สัปดาห์นี้กะดึกมีอะไรเสียบ้าง</button> - เฉพาะกะดึก</li>
                        <li><button onclick="askExample('เดือนก่อนกะดึกมีอะไรเสียบ้าง')" class="text-pink-400 hover:underline">เดือนก่อนกะดึกมีอะไรเสียบ้าง</button> - กะดึกเดือนก่อน</li>
                        <li><button onclick="askExample('LCM วันนี้ขอประวัติการซ่อม')" class="text-pink-400 hover:underline">LCM วันนี้ขอประวัติการซ่อม</button> - ประวัติ Line เฉพาะ</li>
                        <li><button onclick="askExample('ทีม A เครื่องเสียอาการไหนบ้าง')" class="text-pink-400 hover:underline">ทีม A เครื่องเสียอาการไหนบ้าง</button> - กรองตามทีม</li>
                    </ul>
                </div>
                <div class="mb-4">
                    <h5 class="font-semibold text-green-400 mb-2">🔧 ช่างและประสิทธิภาพ</h5>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm">
                        <li><button onclick="askExample('ช่างไหนซ่อมเยอะที่สุดกี่นาที')" class="text-pink-400 hover:underline">ช่างไหนซ่อมเยอะที่สุดกี่นาที</button> - จำนวนงานซ่อมมากที่สุด</li>
                        <li><button onclick="askExample('ช่างคนไหนโดนเรียกมากที่สุดกี่นาที')" class="text-pink-400 hover:underline">ช่างคนไหนโดนเรียกมากที่สุดกี่นาที</button> - calltime รวมมากที่สุด</li>
                        <li><button onclick="askExample('ทีมไหนใช้เวลาซ่อมรวมมากที่สุด')" class="text-pink-400 hover:underline">ทีมไหนใช้เวลาซ่อมรวมมากที่สุด</button> - เวลาซ่อม (ทีม)</li>
                        <li><button onclick="askExample('ทีมไหนใช้เวลาเรียกรวมมากที่สุด')" class="text-pink-400 hover:underline">ทีมไหนใช้เวลาเรียกรวมมากที่สุด</button> - เวลาเรียก (ทีม)</li>
                        <li><button onclick="askExample('ทีม A ใครซ่อมมากที่สุด')" class="text-pink-400 hover:underline">ทีม A ใครซ่อมมากที่สุด</button> - แยกทีม (เวลาซ่อม)</li>
                        <li><button onclick="askExample('ทีม B ใครโดนเรียกมากที่สุด')" class="text-pink-400 hover:underline">ทีม B ใครโดนเรียกมากที่สุด</button> - แยกทีม (เวลาเรียก)</li>
                    </ul>
                    <p class="text-xs text-zinc-400 mt-3 mb-2">ช่างทั้งหมด — คลิกเพื่อดูประวัติการทำงานของช่างนั้น</p>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm max-h-48 overflow-y-auto custom-scrollbar">${(function () {
      const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      const escAttr = (s) => String(s).replace(/\\\\/g, '\\\\\\\\').replace(/'/g, "\\\\'").replace(/"/g, '&quot;');
      return (suggestionTechs || []).map(tech => {
        const q = tech + ' ประวัติการทำงาน';
        return '<li><button onclick="askExample(\'' + escAttr(q) + '\')" class="text-pink-400 hover:underline">' + esc(tech) + ' ประวัติการทำงาน</button></li>';
      }).join('');
    })()}</ul>
                </div>
                <div>
                    <h5 class="font-semibold text-blue-400 mb-2">📅 PM (Preventive Maintenance)</h5>
                    <p class="text-xs text-zinc-400 mb-2">💡 คำถามแนว "มี PM อะไรบ้าง" ระบบจะสรุปจำนวนรายการให้สั้นๆ แล้วกดปุ่ม <b>📋 ดูรายละเอียดเป็นตาราง</b> เพื่อดูรายละเอียดทั้งหมด</p>
                    <ul class="list-disc list-inside space-y-1 text-zinc-300 pl-4 text-sm">
                        <li><button onclick="askExample('เดือนนี้มี PM อะไรบ้าง')" class="text-pink-400 hover:underline">เดือนนี้มี PM อะไรบ้าง</button> - PM ทั้งหมดในเดือนนี้</li>
                        <li><button onclick="askExample('PM เดือนหน้ามีอะไรบ้าง')" class="text-pink-400 hover:underline">PM เดือนหน้ามีอะไรบ้าง</button> - PM เดือนถัดไป</li>
                        <li><button onclick="askExample('ปีนี้มี PM อะไรบ้าง')" class="text-pink-400 hover:underline">ปีนี้มี PM อะไรบ้าง</button> - PM ทั้งปี</li>
                        <li><button onclick="askExample('สัปดาห์นี้มี PM อะไรบ้าง')" class="text-pink-400 hover:underline">สัปดาห์นี้มี PM อะไรบ้าง</button> - PM สัปดาห์นี้</li>
                        <li><button onclick="askExample('แผน PM ปีนี้')" class="text-pink-400 hover:underline">แผน PM ปีนี้</button> - แผน PM ทั้งปี</li>
                        <li><button onclick="askExample('แผน PM PCB C ปี 2026')" class="text-pink-400 hover:underline">แผน PM PCB C ปี 2026</button> - PM เฉพาะ Line</li>
                        <li><button onclick="askExample('ข้อมูลการเลื่อนแผน PM ปี 2026')" class="text-pink-400 hover:underline">ข้อมูลการเลื่อนแผน PM ปี 2026</button> - PM ที่ถูกย้าย</li>
                        <li><button onclick="askExample('สรุป PM ปีนี้')" class="text-pink-400 hover:underline">สรุป PM ปีนี้</button> - สถิติ PM</li>
                        <li><button onclick="askExample('PM ที่ยังไม่เสร็จมีอะไรบ้าง')" class="text-pink-400 hover:underline">PM ที่ยังไม่เสร็จมีอะไรบ้าง</button> - PM ตามสถานะ</li>
                    </ul>
                </div>
            </div>
        `;
}

function closeHelpGuide() {
  document.getElementById('help-modal').classList.add('hidden');
}

function askExample(question) {
  closeHelpGuide();
  userInput.value = question;
  userInput.focus();
}

// กดลูกศรขึ้น (↑) เพื่อเรียกคำถามล่าสุดกลับมาในช่องพิมพ์
let historyInput = '';
document.getElementById('user-input').addEventListener('keydown', function (e) {
  if (e.key === 'Enter') {
    historyInput = this.value.trim();
  }
  if (e.key === 'ArrowUp' && historyInput !== '') {
    e.preventDefault();
    this.value = historyInput;
  }
});

// --- Meta Mode Functions ---
function toggleMode() {
  const body = document.body;
  const modeBtn = document.getElementById('mode-toggle-btn');
  const modeIcon = document.getElementById('mode-toggle-icon');
  const modeText = document.getElementById('mode-toggle-text');
  const teachBtn = document.getElementById('teach-Elin-btn');
  const updateBtn = document.getElementById('update-meta-btn');
  const sendBtn = document.getElementById('send-btn');
  const userInputField = document.getElementById('user-input');

  // ดึงองค์ประกอบ sidebar sections
  const shortcutSection = document.querySelector('aside > div.flex-1 > div:nth-child(2)'); // คำสั่งด่วน
  const toolsSection = document.querySelector('aside > div.flex-1 > div:nth-child(3)'); // เครื่องมือ

  // ดึงองค์ประกอบ header สำหรับเปลี่ยนสี
  const headerTitle = document.querySelector('aside h1');
  const statusIndicator = document.querySelector('.status-indicator');
  const avatarContainer = document.querySelector('aside .w-12.h-12'); // กรอบรูป avatar

  if (currentMode === 'normal') {
    // เข้าสู่ Meta Mode
    currentMode = 'meta';
    modeText.textContent = 'Meta Mode';

    // อัปเดตปุ่มใน Sidebar
    modeBtn.classList.remove('shortcut-btn--pink');
    modeBtn.classList.add('shortcut-btn--blue');
    modeIcon.classList.remove('ph-robot', 'text-pink-400');
    modeIcon.classList.add('ph-lightning', 'text-blue-400');

    // แสดงปุ่มสอน Elin และ Update Embeddings ใน Sidebar
    teachBtn.classList.remove('hidden');
    if (updateBtn) updateBtn.classList.remove('hidden');

    // ซ่อนปุ่มทางซ้ายทั้งหมด ยกเว้น โหมด และ สอน Elin
    if (shortcutSection) shortcutSection.classList.add('hidden');
    if (toolsSection) toolsSection.classList.add('hidden');

    // เปลี่ยนสี header "CMM&CE engineer" เป็นสีน้ำเงิน
    if (headerTitle) {
      const span = headerTitle.querySelector('span');
      if (span) {
        span.classList.remove('text-pink-500');
        span.classList.add('text-blue-500');
      }
      // ใช้ querySelector ค้นหา p ที่อยู่ใน headerTitle หรืออยู่ติดกับมัน
      const p = document.querySelector('header p.text-sm.text-zinc-400') || document.querySelector('header p.text-xs.text-white') || headerTitle.nextElementSibling;
      if (p) {
        p.classList.remove('text-zinc-400');
        p.classList.add('text-white');
      }
    }

    // เปลี่ยน 💖 หัวใจเป็น 💙 และเปลี่ยนสีเป็นน้ำเงิน
    if (statusIndicator) {
      statusIndicator.textContent = '💙';
      statusIndicator.classList.remove('text-pink-400');
      statusIndicator.classList.add('text-blue-400');
    }

    // เปลี่ยนสีกรอบรูป avatar เป็นสีน้ำเงิน
    if (avatarContainer) {
      avatarContainer.classList.remove('shadow-[0_0_15px_rgba(236,72,153,0.3)]', 'border-pink-500/50');
      avatarContainer.classList.add('shadow-[0_0_15px_rgba(59,130,246,0.3)]', 'border-blue-500/50');
    }

    // เปลี่ยน Theme เป็น น้ำเงินดำ
    body.classList.remove('bg-zinc-950');
    body.classList.add('bg-slate-950');

    // ปรับสีปุ่มส่งและสไตล์ต่างๆ ให้เหมาะกับโหมด Meta
    sendBtn.classList.remove('bg-pink-600', 'hover:bg-pink-500', 'group-focus-within:bg-pink-500');
    sendBtn.classList.add('bg-blue-600', 'hover:bg-blue-500', 'group-focus-within:bg-blue-500');

    userInputField.classList.remove('focus:border-pink-500', 'focus:ring-pink-500/50');
    userInputField.classList.add('focus:border-blue-500', 'focus:ring-blue-500/50');

    // แจ้งเตือนการเข้าโหมด
    addBotMessage('<div class="text-blue-400 font-bold mb-1">🔷 ยินดีต้อนรับสู่โหมด Meta</div>ในโหมดนี้หนูจะตอบคำถามจากความรู้ที่พี่ๆ เพิ่มเข้ามาในฐานข้อมูล Meta Database โดยเฉพาะค่ะ<br>ถ้าต้องการเพิ่มความรู้ใหม่ กดปุ่ม <b>"สอน Elin"</b> ที่เมูด้านซ้ายได้เลยนะคะ 🚀', null, null);
  } else {
    // กลับสู่โหมดปกติ
    currentMode = 'normal';
    modeText.textContent = 'โหมดปกติ';

    // อัปเดตปุ่มใน Sidebar
    modeBtn.classList.remove('shortcut-btn--blue');
    modeBtn.classList.add('shortcut-btn--pink');
    modeIcon.classList.remove('ph-lightning', 'text-blue-400');
    modeIcon.classList.add('ph-robot', 'text-pink-400');

    // ซ่อนปุ่มสอน Elin และ Update Embeddings ใน Sidebar
    teachBtn.classList.add('hidden');
    if (updateBtn) updateBtn.classList.add('hidden');

    // แสดงปุ่มทางซ้ายทั้งหมดกลับมา
    if (shortcutSection) shortcutSection.classList.remove('hidden');
    if (toolsSection) toolsSection.classList.remove('hidden');

    // เปลี่ยนสี header "CMM&CE engineer" กลับเป็นสีชมพู
    if (headerTitle) {
      const span = headerTitle.querySelector('span');
      if (span) {
        span.classList.remove('text-blue-500');
        span.classList.add('text-pink-500');
      }
      // หา p เหมือนตอนเข้าโหมด Meta
      const p = document.querySelector('header p.text-sm.text-blue-300') || document.querySelector('header p.text-xs.text-white') || headerTitle.nextElementSibling;
      if (p) {
        p.classList.remove('text-blue-300');
        p.classList.add('text-white');
      }
    }

    // เปลี่ยน � หัวใจกลับเป็น 💖 และเปลี่ยนสีกลับเป็นชมพู
    if (statusIndicator) {
      statusIndicator.textContent = '💖';
      statusIndicator.classList.remove('text-blue-400');
      statusIndicator.classList.add('text-pink-400');
    }

    // เปลี่ยนสีกรอบรูป avatar กลับเป็นสีชมพู
    if (avatarContainer) {
      avatarContainer.classList.remove('shadow-[0_0_15px_rgba(59,130,246,0.3)]', 'border-blue-500/50');
      avatarContainer.classList.add('shadow-[0_0_15px_rgba(236,72,153,0.3)]', 'border-pink-500/50');
    }

    // เปลี่ยน Theme กลับเป็น ค่าเริ่มต้น (ดำชมพู)
    body.classList.remove('bg-slate-950');
    body.classList.add('bg-zinc-950');

    // คืนสีปุ่มส่ง
    sendBtn.classList.remove('bg-blue-600', 'hover:bg-blue-500', 'group-focus-within:bg-blue-500');
    sendBtn.classList.add('bg-pink-600', 'hover:bg-pink-500', 'group-focus-within:bg-pink-500');

    userInputField.classList.remove('focus:border-blue-500', 'focus:ring-blue-500/50');
    userInputField.classList.add('focus:border-pink-500', 'focus:ring-pink-500/50');

    // แจ้งเตือนการออกจากโหมด
    addBotMessage('<div class="text-pink-400 font-bold">💖 กลับสู่โหมดปกติแล้วค่ะ</div>', null, null);
  }
  syncModeTheme(currentMode, true);
}

async function updateMetaEmbeddings() {
  const btn = document.getElementById('update-meta-btn');
  if (!btn) return;
  const originalHtml = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<i class="ph-fill ph-spinner text-emerald-400 animate-spin"></i> กำลังอัปเดต...';
  try {
    const apiBase = (window.location.origin || '').replace(/\/$/, '');
    // ส่งผ่าน /chat endpoint (workaround สำหรับ proxy)
    const res = await fetch(apiBase + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: '', meta_rebuild_embeddings: true })
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data.status === 'success') {
      btn.innerHTML = '<i class="ph-fill ph-check text-emerald-400"></i> Update สำเร็จ!';
      addBotMessage(`✅ Update สำเร็จแล้วค่ะ! 💙`, null, null);
    } else {
      btn.innerHTML = '<i class="ph-fill ph-x text-red-400"></i> อัปเดตไม่สำเร็จ';
      addBotMessage(`❌ อัปเดต Embeddings ไม่สำเร็จ: ${data.message || data.error || 'unknown error'}`, null, null);
    }
  } catch (err) {
    btn.innerHTML = '<i class="ph-fill ph-x text-red-400"></i> เกิดข้อผิดพลาด';
    addBotMessage('❌ ติดต่อ Backend ไม่ได้ค่ะ ตรวจสอบว่า Server รันอยู่', null, null);
  }
  setTimeout(() => {
    btn.innerHTML = originalHtml;
    btn.disabled = false;
  }, 3000);
}

function openTeachModal() {
  // แสดง login modal ก่อน
  const loginModal = document.getElementById('login-modal');
  loginModal.classList.remove('hidden');

  // ป้องกันการ Tab ออกจาก modal
  trapFocus(loginModal);
}

function closeTeachModal() {
  document.getElementById('teach-modal').classList.add('hidden');
  document.getElementById('teach-form').reset();
  removeFocusTrap();
}

function closeLoginModal() {
  document.getElementById('login-modal').classList.add('hidden');
  document.getElementById('login-form').reset();
  removeFocusTrap();
}

// ฟังก์ชันป้องกันการ Tab ออกจาก modal
function trapFocus(modal) {
  const focusableElements = modal.querySelectorAll(
    'input, button, textarea, select, a[href], [tabindex]:not([tabindex="-1"])'
  );
  const firstElement = focusableElements[0];
  const lastElement = focusableElements[focusableElements.length - 1];

  modal.addEventListener('keydown', handleFocusTrap);

  function handleFocusTrap(e) {
    if (e.key !== 'Tab') return;

    if (e.shiftKey) {
      if (document.activeElement === firstElement) {
        e.preventDefault();
        lastElement.focus();
      }
    } else {
      if (document.activeElement === lastElement) {
        e.preventDefault();
        firstElement.focus();
      }
    }
  }

  // เก็บ reference เพื่อลบทีหลัง
  modal._focusTrapHandler = handleFocusTrap;

  // Focus ที่ element แรก
  setTimeout(() => firstElement?.focus(), 100);
}

function removeFocusTrap() {
  const loginModal = document.getElementById('login-modal');
  const teachModal = document.getElementById('teach-modal');

  if (loginModal._focusTrapHandler) {
    loginModal.removeEventListener('keydown', loginModal._focusTrapHandler);
    delete loginModal._focusTrapHandler;
  }

  if (teachModal._focusTrapHandler) {
    teachModal.removeEventListener('keydown', teachModal._focusTrapHandler);
    delete teachModal._focusTrapHandler;
  }
}

// จัดการ Login Form
document.getElementById('login-form')?.addEventListener('submit', async (e) => {
  e.preventDefault();

  const user = document.getElementById('login-user').value;
  const password = document.getElementById('login-password').value;

  // ตรวจสอบ login
  if (user === 'admin' && password === '1122334455') {
    // ปิด login modal
    closeLoginModal();
    // เปิด teach modal
    const teachModal = document.getElementById('teach-modal');
    teachModal.classList.remove('hidden');
    trapFocus(teachModal);
  } else {
    // แสดง error notification
    showLoginError();
  }
});

// แสดง error notification
function showLoginError() {
  const notification = document.getElementById('login-error-notification');
  notification.classList.remove('hidden');

  // ซ่อนอัตโนมัติหลัง 4 วินาที
  setTimeout(() => {
    closeLoginError();
  }, 4000);
}

// ปิด error notification
function closeLoginError() {
  const notification = document.getElementById('login-error-notification');
  notification.classList.add('hidden');
}

// จัดการการส่งข้อมูลสอน Elin
document.getElementById('teach-form')?.addEventListener('submit', async (e) => {
  e.preventDefault();

  const name = document.getElementById('teach-name').value;
  const topic = document.getElementById('teach-topic').value;
  const answer = document.getElementById('teach-answer').value;
  const submitBtn = e.target.querySelector('button[type="submit"]');

  // แสดง loading animation แบบเดียวกับตอนถามคำถาม
  const loadingId = addLoading({
    mode: 'meta',
    stage: 'META_SAVE'
  });

  // ซ่อนปุ่ม submit
  submitBtn.style.display = 'none';

  try {
    // ส่งผ่าน /chat endpoint (workaround สำหรับ proxy)
    const apiBase = (window.location.origin || '').replace(/\/$/, '');
    const res = await fetch(apiBase + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: '',
        meta_add: { name, topic, answer }
      })
    });

    const text = await res.text();
    let data = {};
    try {
      data = JSON.parse(text);
    } catch (e) {
      console.error('Failed to parse response:', text);
      throw new Error('Invalid JSON response from server');
    }

    // ลบ loading animation
    removeLoading(loadingId);

    if (res.ok && data.status === 'success') {
      // สำเร็จ
      closeTeachModal();
      // แจ้งในแชทให้รู้
      addBotMessage(`<div class="text-green-400 font-bold mb-1">✅ รับทราบความรู้ใหม่!</div><b>หัวข้อ:</b> ${escapeHtml(topic)}<br>หนูได้บันทึกความรู้จากพี่ ${escapeHtml(name)} ลงในระบบ Meta เรียบร้อยแล้วค่ะ! 💖`, null, null);
    } else {
      // แสดง error message จาก backend
      const errorMsg = data.message || data.detail || 'ไม่ทราบสาเหตุ';
      alert('❌ เกิดข้อผิดพลาด: ' + errorMsg);
      console.error('Meta add error:', data);
    }
  } catch (err) {
    // ลบ loading animation
    removeLoading(loadingId);
    console.error('Error adding meta:', err);
    alert('❌ เชื่อมต่อเซิร์ฟเวอร์ไม่ได้ค่ะ\nError: ' + err.message);
  } finally {
    // แสดงปุ่ม submit กลับมา
    submitBtn.style.display = '';
  }
});

// 24. Music Toggle Logic
function toggleMusic() {
  const music = document.getElementById('bg-music');
  const btn = document.getElementById('music-toggle');
  const icon = btn.querySelector('i');
  const sticker = btn.querySelector('.music-sticker');

  if (music.paused) {
    music.play().then(() => {
      icon.className = 'ph-fill ph-music-notes text-pink-500';
      sticker.textContent = '🎵';
      btn.classList.add('music-btn--playing');
    }).catch(e => {
      console.error("Audio play failed:", e);
      alert("ไม่สามารถเล่นเพลงได้ กรุณาตรวจสอบไฟล์ Elin.mp3 ในโฟลเดอร์ static ค่ะ");
    });
  } else {
    music.pause();
    icon.className = 'ph ph-speaker-none text-zinc-500';
    sticker.textContent = '✨';
    btn.classList.remove('music-btn--playing');
  }
}

// --- Text-to-Speech (TTS) Logic ---
let currentTtsAudio = null;
let currentSpeakingBtn = null;

async function toggleReadAloud(btn) {
  if (currentTtsAudio && !currentTtsAudio.paused && currentSpeakingBtn === btn) {
    currentTtsAudio.pause();
    btn.classList.remove('is-speaking');
    currentSpeakingBtn = null;
    return;
  }

  // Stop previous
  if (currentTtsAudio) {
    currentTtsAudio.pause();
    currentTtsAudio = null;
  }
  if (currentSpeakingBtn) {
    currentSpeakingBtn.classList.remove('is-speaking');
  }

  // Find the message bubble - try closest first, then sibling
  let bubble = btn.closest('.msg-bot');
  
  // If button is external (outside .msg-bot), look for sibling or nearby .msg-bot
  if (!bubble) {
    // Try to find .msg-bot as a sibling or in parent
    const parent = btn.parentElement;
    if (parent) {
      bubble = parent.querySelector('.msg-bot') || parent.closest('.msg-bot');
    }
  }
  
  // If still not found, try looking in the previous sibling
  if (!bubble && btn.previousElementSibling) {
    bubble = btn.previousElementSibling.classList.contains('msg-bot') 
      ? btn.previousElementSibling 
      : btn.previousElementSibling.querySelector('.msg-bot');
  }
  
  if (!bubble) {
    console.error('TTS button: Could not find associated .msg-bot element');
    return;
  }
  
  const contentEl = bubble.querySelector('.msg-content') || bubble;
  
  // Extract text, excluding action buttons and debug info
  let textToRead = "";
  const tempDiv = document.createElement('div');
  tempDiv.innerHTML = contentEl.innerHTML;
  
  // Remove extra elements we don't want to read
  const toRemove = tempDiv.querySelectorAll('.msg-bot-actions, .msg-bot-debug, details, button, .speaker-waves');
  toRemove.forEach(el => el.remove());
  
  textToRead = tempDiv.innerText.trim();
  if (!textToRead) return;

  // Define browser TTS fallback function
  function tryBrowserTTS() {
    // Fallback 1: Use browser's built-in Speech Synthesis API
    if ('speechSynthesis' in window) {
      const voices = speechSynthesis.getVoices();
      console.log('Available voices:', voices.map(v => `${v.name} (${v.lang}) ${v.gender || ''}`));
      
      // หาเสียงไทย
      const thaiVoice = voices.find(v => v.lang.includes('th')) || 
                       voices.find(v => v.lang.includes('TH'));
      
      if (thaiVoice) {
        console.log('Using Thai voice:', thaiVoice.name);
        const utterance = new SpeechSynthesisUtterance(textToRead);
        utterance.voice = thaiVoice;
        utterance.lang = 'th-TH';
        utterance.rate = 0.6;    // ความเร็ว: ช้ากว่าปกติ 30%
        utterance.pitch = 4;   // ระดับเสียง: สูงกว่าปกติ 50%
        utterance.volume = 1.0;  // ความดัง: 100% (ดังสุด)
        
        utterance.onend = () => {
          btn.classList.remove('is-speaking');
          currentSpeakingBtn = null;
        };
        
        utterance.onerror = () => {
          btn.classList.remove('is-speaking');
          currentSpeakingBtn = null;
          console.error("Browser TTS failed");
        };
        
        speechSynthesis.speak(utterance);
      } else {
        console.log('No Thai voice found, trying Google TTS...');
        
        // Fallback 2: Use Google Translate TTS
        try {
          const googleTtsUrl = `https://translate.google.com/translate_tts?ie=UTF-8&tl=th&client=tw-ob&q=${encodeURIComponent(textToRead)}`;
          currentTtsAudio = new Audio(googleTtsUrl);
          
          currentTtsAudio.onended = () => {
            btn.classList.remove('is-speaking');
            currentSpeakingBtn = null;
          };
          
          currentTtsAudio.onerror = () => {
            btn.classList.remove('is-speaking');
            currentSpeakingBtn = null;
            console.error("Google TTS failed");
            alert('ขออภัยค่ะ ระบบอ่านเสียงไม่พร้อมใช้งาน กรุณาติดตั้ง Thai Voice Pack ในระบบค่ะ');
          };
          
          currentTtsAudio.play();
        } catch (googleErr) {
          console.error('Google TTS failed:', googleErr);
          alert('ขออภัยค่ะ ระบบอ่านเสียงไม่พร้อมใช้งาน\n\nแนะนำ: ติดตั้ง Thai Voice Pack\nSettings → Language → Thai → Speech → Download');
        }
      }
    } else {
      console.error('No TTS available');
      alert('ขออภัยค่ะ ระบบอ่านเสียงไม่พร้อมใช้งานในขณะนี้');
    }
  }

  try {
    btn.classList.add('is-speaking');
    currentSpeakingBtn = btn;

    // Detect proxy path and construct correct API URL
    const currentPath = window.location.pathname;
    let apiPath = '/api/tts';
    
    // If we're under a proxy path like /ai/, include it in the API path
    if (currentPath.startsWith('/ai/')) {
      apiPath = '/ai/api/tts';
    }
    
    let audioUrl = `${apiPath}?text=${encodeURIComponent(textToRead)}`;
    
    currentTtsAudio = new Audio(audioUrl);
    
    currentTtsAudio.onended = () => {
      btn.classList.remove('is-speaking');
      currentSpeakingBtn = null;
    };
    
    currentTtsAudio.onerror = () => {
      btn.classList.remove('is-speaking');
      currentSpeakingBtn = null;
      console.log("Server TTS failed, trying browser TTS...");
      
      // Fallback to browser TTS when server TTS fails
      tryBrowserTTS();
    };

    await currentTtsAudio.play();
  } catch (err) {
    console.log('Server TTS failed, trying browser TTS...');
    tryBrowserTTS();
  }
}

// --- 🌓 Sidebar Toggle Logic ---
function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar) return;
  
  const isCollapsed = sidebar.classList.toggle('sidebar-collapsed');
  
  // Save state to localStorage
  localStorage.setItem('sidebar-collapsed', isCollapsed ? 'true' : 'false');
  
  // Optional: Trigger resize for charts if open
  if (window.currentChart) {
    setTimeout(() => window.currentChart.resize(), 305);
  }
}

// Initial sidebar state
document.addEventListener('DOMContentLoaded', () => {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar) return;
  
  const savedState = localStorage.getItem('sidebar-collapsed');
  if (savedState === 'true') {
    sidebar.classList.add('sidebar-collapsed');
  }
});
