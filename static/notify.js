(function(){
  // In-app polling notifications: toast + optional sound.
  // This complements OneSignal push and works even if the user denies push.

  const uid = (window.AGROMATH && window.AGROMATH.uid) ? String(window.AGROMATH.uid) : "";
  if (!uid) return;

  const box = document.getElementById('notifbox');
  if (!box) return;

  const LAST_KEY = 'agromath_notif_last_id_v1';
  let last = 0;
  try { last = parseInt(localStorage.getItem(LAST_KEY) || '0', 10) || 0; } catch(e) {}

  // Preload sound (may still be blocked by autoplay policies until user interacts)
  const audio = new Audio('/static/notify.wav');
  audio.preload = 'auto';

  function toast(msg, kind, link){
    const t = document.createElement('div');
    t.className = 'toast ' + (kind === 'warn' ? 'warn' : (kind === 'danger' ? 'danger' : 'ok'));
    const a = link ? `<a href="${link}" style="text-decoration:underline;">Open</a>` : '';
    t.innerHTML = `<div style="font-weight:800;margin-bottom:4px;">Notification</div><div>${escapeHtml(msg)}</div>${a ? `<div style="margin-top:6px;">${a}</div>` : ''}`;
    box.appendChild(t);

    // Auto dismiss
    setTimeout(()=>{
      try { t.style.opacity = '0'; t.style.transition = 'opacity .35s ease'; } catch(e){}
      setTimeout(()=>{ try { t.remove(); } catch(e){} }, 450);
    }, 9000);
  }

  function escapeHtml(str){
    return String(str || '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }

  async function poll(){
    try {
      const res = await fetch(`/api/notifications?since=${encodeURIComponent(String(last))}`, {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      });
      if (!res.ok) return;
      const data = await res.json();
      if (!data || !Array.isArray(data.items)) return;

      if (data.items.length){
        // Play sound once per poll batch
        try { await audio.play(); } catch(e) {}
      }

      for (const it of data.items){
        const id = Number(it.id || 0);
        if (id > last) last = id;
        const kind = (String(it.kind || '').toUpperCase() === 'NEW_ORDER') ? 'ok'
          : (String(it.kind || '').toUpperCase().includes('DECLINED') ? 'danger'
          : (String(it.kind || '').toUpperCase().includes('DELIVERED') ? 'ok'
          : 'warn'));
        toast(it.message, kind, it.link);
      }

      try { localStorage.setItem(LAST_KEY, String(last)); } catch(e) {}
    } catch (e) {
      // fail silent
    }
  }

  // First poll shortly after load, then every 12s.
  setTimeout(poll, 1500);
  setInterval(poll, 12000);
})();
