/* ═══════════════════════════════════════════════════════════
   Allocation / Details Tab — Professional Table View
   ═══════════════════════════════════════════════════════════ */

let _detailsLoaded = false;

document.addEventListener('DOMContentLoaded', () => {
    const detailsTab = document.getElementById('detailsTabLink');
    detailsTab.addEventListener('shown.bs.tab', () => {
        if (!_detailsLoaded) { _detailsLoaded = true; fetchDetailsData(); }
    });
});

async function fetchDetailsData() {
    const container = document.getElementById('detailsContainer');

    try {
        const response = await fetch('/api/details/data');
        const result = await response.json();

        if (result.status === 'success' && result.data && result.data.length > 0) {
            renderDetailsView(container, result.data);
        } else {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="bi bi-table"></i>
                    <p>No allocation data available</p>
                </div>`;
        }
    } catch (error) {
        console.error('Error:', error);
        container.innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle"></i>
                <p>Error loading allocation data</p>
            </div>`;
    }
}

function renderDetailsView(container, data) {
    const columns = Object.keys(data[0]);

    // Hidden/internal columns
    const hideSet = new Set(['id', 'uuid', 'customAttributes', 'createdDate', 'modifiedDate']);
    const visibleCols = columns.filter(c => !hideSet.has(c));

    // Sort data by employee name or first text column
    const sortCol = visibleCols.find(c =>
        c.toLowerCase().includes('employee') || c.toLowerCase().includes('name')
    ) || visibleCols[0];

    data.sort((a, b) => {
        let va = a[sortCol] || '', vb = b[sortCol] || '';
        return String(va).localeCompare(String(vb), undefined, { sensitivity: 'base' });
    });

    // Build filter bar
    const filterHTML = `
        <div class="filter-bar">
            <input type="text" id="detailsSearch" placeholder="Search allocations..."
                   style="flex:1;max-width:300px">
            <span style="color:var(--clr-text-muted);font-size:0.84rem" id="detailsRowCount">
                ${data.length} record${data.length !== 1 ? 's' : ''}
            </span>
        </div>
    `;

    // Build table
    const headerHTML = visibleCols.map(c => `<th>${_formatDetLabel(c)}</th>`).join('');
    const bodyHTML = data.map((row, i) => {
        const tds = visibleCols.map(c => {
            const val = _formatDetValue(c, row[c]);
            return `<td>${val}</td>`;
        }).join('');
        return `<tr data-search="${_getSearchText(row, visibleCols)}">${tds}</tr>`;
    }).join('');

    container.innerHTML = `
        ${filterHTML}
        <table class="pro-table">
            <thead><tr>${headerHTML}</tr></thead>
            <tbody id="detailsBody">${bodyHTML}</tbody>
        </table>
    `;

    // Wire up search
    document.getElementById('detailsSearch').addEventListener('input', (e) => {
        const q = e.target.value.trim().toLowerCase();
        const rows = document.querySelectorAll('#detailsBody tr');
        let visible = 0;
        rows.forEach(tr => {
            const match = !q || tr.dataset.search.includes(q);
            tr.style.display = match ? '' : 'none';
            if (match) visible++;
        });
        document.getElementById('detailsRowCount').textContent =
            `${visible} record${visible !== 1 ? 's' : ''}`;
    });
}

/* ── Helpers ───────────────────────────────────────────── */

function _formatDetLabel(key) {
    return key
        .replace(/([A-Z])/g, ' $1')
        .replace(/_/g, ' ')
        .replace(/^\w/, c => c.toUpperCase())
        .trim();
}

function _formatDetValue(key, val) {
    if (val === null || val === undefined || val === '' || val === 'None') return '—';

    const s = String(val);

    // Dates
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
        try {
            const d = new Date(s);
            if (!isNaN(d.getTime()) && d.getFullYear() > 1900) {
                return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
            }
        } catch {/* fallback */}
        if (s.startsWith('0001-') || s.startsWith('1900-')) return '—';
    }

    // Status badges
    const lower = s.toLowerCase();
    if (lower === 'active' || lower === 'allocated')
        return `<span class="status-badge badge-active">${_escD(s)}</span>`;
    if (lower === 'completed' || lower === 'closed')
        return `<span class="status-badge badge-completed">${_escD(s)}</span>`;
    if (lower === 'on hold' || lower === 'onhold')
        return `<span class="status-badge badge-onhold">${_escD(s)}</span>`;
    if (lower === 'on bench' || lower === 'bench')
        return `<span class="status-badge badge-inactive">${_escD(s)}</span>`;

    // Booleans
    if (lower === 'true') return '<span class="status-badge badge-true">Yes</span>';
    if (lower === 'false') return '<span class="status-badge badge-false">No</span>';

    // JSON arrays/objects — extract readable text
    try {
        const parsed = JSON.parse(s);
        if (Array.isArray(parsed)) {
            return parsed.map(item =>
                typeof item === 'object' ? (item.name || item.displayName || JSON.stringify(item)) : String(item)
            ).join(', ') || '—';
        }
        if (typeof parsed === 'object' && parsed !== null) {
            return parsed.name || parsed.displayName || JSON.stringify(parsed);
        }
    } catch {/* not JSON */}

    return _escD(s);
}

function _getSearchText(row, cols) {
    return cols.map(c => String(row[c] || '').toLowerCase()).join(' ');
}

function _escD(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
