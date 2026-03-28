/**
 * Groovon — Client-side interactions
 * HTMX extensions, file upload, toast notifications, theme utils
 */

/* ── Toast Notifications ───────────────────────────────────── */

function showToast(message, type = 'info', duration = 4000) {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.cssText = `
            position: fixed; bottom: 24px; right: 24px; z-index: 9999;
            display: flex; flex-direction: column-reverse; gap: 8px;
        `;
        document.body.appendChild(container);
    }

    const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
    const colors = {
        success: 'rgba(16,185,129,.15)',
        error: 'rgba(239,68,68,.15)',
        info: 'rgba(99,102,241,.15)',
        warning: 'rgba(245,158,11,.15)',
    };
    const borders = {
        success: '#10b981',
        error: '#ef4444',
        info: '#6366f1',
        warning: '#f59e0b',
    };

    const toast = document.createElement('div');
    toast.style.cssText = `
        background: ${colors[type] || colors.info};
        border: 1px solid ${borders[type] || borders.info};
        color: #e2e8f0;
        padding: 12px 20px;
        border-radius: 12px;
        font-size: 14px;
        backdrop-filter: blur(12px);
        animation: slideInRight 0.3s ease;
        cursor: pointer;
        max-width: 360px;
    `;
    toast.textContent = `${icons[type] || ''} ${message}`;
    toast.onclick = () => toast.remove();

    container.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        toast.style.transition = 'all 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

/* ── Drag & Drop File Upload ───────────────────────────────── */

function initDropZone() {
    const zone = document.getElementById('drop-zone');
    const input = document.getElementById('file-input');
    if (!zone || !input) return;

    ['dragenter', 'dragover'].forEach(e =>
        zone.addEventListener(e, ev => {
            ev.preventDefault();
            zone.classList.add('drag-over');
        })
    );

    ['dragleave', 'drop'].forEach(e =>
        zone.addEventListener(e, ev => {
            ev.preventDefault();
            zone.classList.remove('drag-over');
        })
    );

    zone.addEventListener('drop', ev => {
        const files = ev.dataTransfer.files;
        if (files.length) {
            input.files = files;
            updateFileName(files[0].name);
        }
    });

    zone.addEventListener('click', () => input.click());

    input.addEventListener('change', () => {
        if (input.files.length) {
            updateFileName(input.files[0].name);
        }
    });
}

function updateFileName(name) {
    const label = document.getElementById('file-name');
    if (label) {
        label.textContent = `📎 ${name}`;
        label.style.display = 'block';
    }
}

/* ── Tab Switching (Job Create) ────────────────────────────── */

function initTabs() {
    const tabs = document.querySelectorAll('[data-tab]');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.dataset.tab;

            // Update active tab
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            // Show target panel
            document.querySelectorAll('.tab-panel').forEach(p => {
                p.hidden = p.id !== `panel-${target}`;
            });

            // Update hidden source_type field
            const sourceInput = document.getElementById('source-type');
            if (sourceInput) sourceInput.value = target;
        });
    });
}

/* ── HTMX Event Listeners ─────────────────────────────────── */

document.addEventListener('htmx:afterSwap', (event) => {
    // Re-initialise anything in swapped content
});

document.addEventListener('htmx:responseError', () => {
    showToast('Request failed — please try again', 'error');
});

/* ── Keyboard Shortcut — press N for New Job ──────────────── */

document.addEventListener('keydown', (e) => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    if (e.key === 'n' && !e.ctrlKey && !e.metaKey) {
        window.location.href = '/jobs/create/';
    }
});

/* ── Init ──────────────────────────────────────────────────── */

document.addEventListener('DOMContentLoaded', () => {
    initDropZone();
    initTabs();
});
