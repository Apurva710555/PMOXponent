/* ═══════════════════════════════════════════════════════════
   Project Tab — Filter Dropdowns + Detail Cards + STATUS TOP
   ═══════════════════════════════════════════════════════════ */

let _projData = [];
let _projLoaded = false;
let _currentProjIdx = 0; // index of currently displayed project
let _activeProjSubTab = "overview"; // tracks which sub-tab is active

document.addEventListener("DOMContentLoaded", () => {
  const projTab = document.getElementById("projTabLink");
  projTab.addEventListener("shown.bs.tab", () => {
    if (!_projLoaded) {
      _projLoaded = true;
      fetchProjectData();
      _fetchAccountManagers();
    }
  });

  _setupAccountManagerEvents();
});

/* ═══ FETCH DATA ═══════════════════════════════════════════ */
async function fetchProjectData() {
  try {
    const response = await fetch("/api/project/data");
    const result = await response.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      let previousId = null;
      if (_projData.length > 0 && _currentProjIdx < _projData.length) {
        previousId = _projData[_currentProjIdx]?.id;
      }

      _projData = result.data;
      _buildProjFilters();

      let targetIdx = 0;
      if (previousId) {
        const found = _projData.findIndex((p) => p.id === previousId);
        if (found !== -1) targetIdx = found;
      }

      _showProject(targetIdx);
    } else {
      document.getElementById("projDetailPanel").innerHTML = `
                <div class="empty-state">
                    <i class="bi bi-folder"></i>
                    <p>No project data available</p>
                </div>`;
      document.getElementById("projFilterName").innerHTML =
        '<option value="">No data</option>';
      document.getElementById("projFilterClient").innerHTML =
        '<option value="">No data</option>';
    }
  } catch (error) {
    console.error("Error:", error);
    document.getElementById("projDetailPanel").innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle"></i>
                <p>Error loading project data</p>
            </div>`;
  }
}

/* ═══ STATUS MAPPING ═══════════════════════════════════════ */

function _getStatusText(val) {
  if (val === 0 || val === "0") return "On-going";
  if (val === 1 || val === "1") return "Completed";
  return "Unknown";
}

/* ═══ Filter Dropdowns ═════════════════════════════════ */

let _clientNameMap = {};
let _clientCodeMap = {};
let _projNameMap = {};
let _projCodeMap = {};

function _buildProjFilters() {
  const clientNameInput = document.getElementById("projFilterClientName");
  const clientCodeInput = document.getElementById("projFilterClientCode");
  const projNameInput = document.getElementById("projFilterName");
  const projCodeInput = document.getElementById("projFilterCode");

  const clientNameList = document.getElementById("projClientNameList");
  const clientCodeList = document.getElementById("projClientCodeList");
  const projNameList = document.getElementById("projNameList");
  const projCodeList = document.getElementById("projCodeList");

  const clientNames = new Set();
  const clientCodes = new Set();

  _clientNameMap = {};
  _clientCodeMap = {};
  _projNameMap = {};
  _projCodeMap = {};

  _projData.forEach((p, i) => {
    const cName = p.clientName || "Untitled Client";
    const cCode = p.clientCode || "No Code";
    const pName = p.name || "Untitled";
    const pCode = p.code || "";

    clientNames.add(cName);
    clientCodes.add(cCode);

    if (!_clientNameMap[cName]) _clientNameMap[cName] = [];
    _clientNameMap[cName].push(i);

    if (!_clientCodeMap[cCode]) _clientCodeMap[cCode] = [];
    _clientCodeMap[cCode].push(i);

    _projNameMap[pName] = i;
    if (pCode) _projCodeMap[pCode] = i;
  });

  clientNameList.innerHTML = Array.from(clientNames).sort().map(v => `<option value="${_escP(v)}">`).join("");
  clientCodeList.innerHTML = Array.from(clientCodes).sort().map(v => `<option value="${_escP(v)}">`).join("");

  function updateProjectDropdowns(allowedIndices) {
    const pNames = new Set();
    const pCodes = new Set();
    allowedIndices.forEach(i => {
      const p = _projData[i];
      if (p.name) pNames.add(p.name);
      if (p.code) pCodes.add(p.code);
    });
    projNameList.innerHTML = Array.from(pNames).sort().map(v => `<option value="${_escP(v)}">`).join("");
    projCodeList.innerHTML = Array.from(pCodes).sort().map(v => `<option value="${_escP(v)}">`).join("");
  }

  // Initialize with all projects
  updateProjectDropdowns(_projData.map((_, i) => i));

  // Set first project values
  const firstProj = _projData[0];
  if (firstProj) {
    if (clientNameInput) clientNameInput.value = firstProj.clientName || "Untitled Client";
    if (clientCodeInput) clientCodeInput.value = firstProj.clientCode || "No Code";
    if (projNameInput) projNameInput.value = firstProj.name || "";
    if (projCodeInput) projCodeInput.value = firstProj.code || "";
    
    const allowed = _clientNameMap[clientNameInput.value] || _projData.map((_, i) => i);
    updateProjectDropdowns(allowed);
  }

  // Event Listeners
  clientNameInput.addEventListener("change", () => {
    const indices = _clientNameMap[clientNameInput.value];
    if (indices && indices.length > 0) {
      const firstIdx = indices[0];
      clientCodeInput.value = _projData[firstIdx].clientCode || "No Code";
      updateProjectDropdowns(indices);
      
      projNameInput.value = _projData[firstIdx].name || "";
      projCodeInput.value = _projData[firstIdx].code || "";
      _showProject(firstIdx);
    }
  });

  clientCodeInput.addEventListener("change", () => {
    const indices = _clientCodeMap[clientCodeInput.value];
    if (indices && indices.length > 0) {
      const firstIdx = indices[0];
      clientNameInput.value = _projData[firstIdx].clientName || "Untitled Client";
      updateProjectDropdowns(indices);

      projNameInput.value = _projData[firstIdx].name || "";
      projCodeInput.value = _projData[firstIdx].code || "";
      _showProject(firstIdx);
    }
  });

  projNameInput.addEventListener("change", () => {
    const idx = _projNameMap[projNameInput.value];
    if (idx !== undefined) {
      const p = _projData[idx];
      projCodeInput.value = p.code || "";
      clientNameInput.value = p.clientName || "Untitled Client";
      clientCodeInput.value = p.clientCode || "No Code";
      
      const indices = _clientNameMap[clientNameInput.value] || [];
      updateProjectDropdowns(indices);
      _showProject(idx);
    }
  });

  projCodeInput.addEventListener("change", () => {
    const idx = _projCodeMap[projCodeInput.value];
    if (idx !== undefined) {
      const p = _projData[idx];
      projNameInput.value = p.name || "";
      clientNameInput.value = p.clientName || "Untitled Client";
      clientCodeInput.value = p.clientCode || "No Code";

      const indices = _clientNameMap[clientNameInput.value] || [];
      updateProjectDropdowns(indices);
      _showProject(idx);
    }
  });
}

/* ═══ Render Project Detail ════════════════════════════ */

function _showProject(idx) {
  _currentProjIdx = idx;
  const proj = _projData[idx];

  const clientNameInput = document.getElementById("projFilterClientName");
  const clientCodeInput = document.getElementById("projFilterClientCode");
  const projNameInput = document.getElementById("projFilterName");
  const projCodeInput = document.getElementById("projFilterCode");

  if (proj && clientNameInput) clientNameInput.value = proj.clientName || "Untitled Client";
  if (proj && clientCodeInput) clientCodeInput.value = proj.clientCode || "No Code";
  if (proj && projNameInput) projNameInput.value = proj.name || "";
  if (proj && projCodeInput) projCodeInput.value = proj.code || "";

  // Always reset to Overview sub-tab when switching projects
  switchProjTab("overview");

  // Clear Resources panel so next click re-fetches for the new project
  const rPanel = document.getElementById("projResourcesPanel");
  if (rPanel) {
    rPanel.innerHTML = `<div class="empty-state"><i class="bi bi-people"></i><p>Click Resources to load allocation data.</p></div>`;
  }

  const panel = document.getElementById("projDetailPanel");

  const mgrInput = document.getElementById("projManagerSelect");
  if (mgrInput) {
    mgrInput.value = proj.accountmanager || "";
  }

  const overviewFields = ["name", "code", "billingType", "clientId"];
  const timelineFields = ["budgetedTime"];

  // Dynamically catch any column name variations for start/end dates from Databricks
  // and route them immediately into the timeline card to completely avoid "Other Details"
  const projKeys = Object.keys(proj);
  const startKeys = projKeys.filter(
    (k) =>
      k.toLowerCase().replace(/_/g, "").replace(/ /g, "") ===
      "projectstartdate",
  );
  const endKeys = projKeys.filter(
    (k) =>
      k.toLowerCase().replace(/_/g, "").replace(/ /g, "") === "projectenddate",
  );

  // Push whatever exact keys Databricks provided into the timeline array
  timelineFields.unshift(...startKeys, ...endKeys);

  const statusFields = ["isArchived", "isBillable"];
  const budgetFields = ["projectBudget"];
  const managersFields = ["projectManagers"];

  const allGrouped = new Set([
    ...overviewFields,
    ...timelineFields,
    ...statusFields,
    ...budgetFields,
    ...managersFields,
  ]);
  // Skip duplicate/internal fields from Databricks SCD2 logic / legacy fields
  const skipFields = new Set([
    "id",
    "customAttributes",
    "status",
    "startdate",
    "enddate",
    "modify",
    "comments",
    "accountmanager",
  ]);
  const otherFields = projKeys.filter(
    (k) =>
      !allGrouped.has(k) &&
      !skipFields.has(k.toLowerCase()) &&
      !skipFields.has(k),
  );

  // panel.innerHTML = `
  //     ${_buildProjCard('Project Overview', 'bi-folder-fill', overviewFields, proj)}
  //     ${_buildProjCard('Timeline', 'bi-calendar-range', timelineFields, proj)}
  //     ${_buildProjCard('Status', 'bi-toggle-on', statusFields, proj)}
  //     ${_buildProjCard('Budget', 'bi-cash-stack', budgetFields, proj)}
  //     ${_buildProjCard('Project Managers', 'bi-person-workspace', managersFields, proj)}
  //     ${otherFields.length ? _buildProjCard('Other Details', 'bi-three-dots', otherFields, proj) : ''}
  // `;
  panel.innerHTML = (() => {
    // Pick helper — first non-empty value
    const _pv = (val) => {
      const v = _formatProjValue("", val === undefined ? "" : val);
      return v &&
        v !== '<span style="color:var(--clr-text-muted)">Not set</span>'
        ? v
        : "";
    };

    // Flat section builder (mirrors employee tab style)
    function _flatProjSec(heading, icon, pairs) {
      const cells = pairs
        .map(([lbl, rawKey]) => {
          const raw = proj[rawKey];
          const formatted = _formatProjValue(rawKey, raw);
          if (!formatted || formatted.includes("Not set")) return "";
          return `
            <div class="flat-field">
              <span class="flat-lbl">${lbl}</span>
              <span class="flat-val">${formatted}</span>
            </div>`;
        })
        .join("");
      if (!cells.trim()) return "";
      return `
          <div class="flat-section">
            <div class="flat-section-hdr"><i class="bi ${icon}"></i> ${heading}</div>
            <div class="flat-grid">${cells}</div>
          </div>`;
    }

    // Detect actual start/end date keys from Databricks (key names vary)
    const projKeys = Object.keys(proj);
    const startKey =
      projKeys.find(
        (k) =>
          k.toLowerCase().replace(/_/g, "").replace(/ /g, "") ===
          "projectstartdate",
      ) || "";
    const endKey =
      projKeys.find(
        (k) =>
          k.toLowerCase().replace(/_/g, "").replace(/ /g, "") ===
          "projectenddate",
      ) || "";

    return `
        ${_flatProjSec("Project Details", "bi-folder-fill", [
          ["Project Name", "name"],
          ["Project Code", "code"],
          ["Client Name", "clientName"],
          ["Client Code", "clientCode"],
          ["Client Desc", "clientDescription"],
          ["Status", "status"],
          //   ['Client Address',   'clientAddress'],
          ["Billing Type", "billingType"],
          ["Billable", "isBillable"],
          ["Archived", "isArchived"],
          ["Project Managers", "projectManagers"],
          ["Account Manager", "accountmanager"],
        ])}

        ${_flatProjSec("Timeline & Budget", "bi-calendar-range", [
          ["Start Date", startKey],
          ["End Date", endKey],
          ["Budgeted Time", "budgetedTime"],
          ["Budget", "projectBudget"],
        ])}
      `;
  })();
}

/* ═══ Helpers ══════════════════════════════════════════ */

const PROJ_LABEL_OVERRIDES = {
  billingType: "Billing Type",
  clientId: "Client ID",
  budgetedTime: "Budgeted Time",
  isArchived: "Archived",
  isBillable: "Billable",
  projectBudget: "Project Budget",
  projectManagers: "Project Managers",
};

function _formatProjLabel(key) {
  if (PROJ_LABEL_OVERRIDES[key]) return PROJ_LABEL_OVERRIDES[key];

  // Hard override for date fields regardless of Databricks casing/spacing
  const cleanKey = key.toLowerCase().replace(/_/g, "").replace(/ /g, "");
  if (cleanKey === "projectstartdate") return "Start Date";
  if (cleanKey === "projectenddate") return "End Date";

  return key
    .replace(/([A-Z])/g, " $1")
    .replace(/_/g, " ")
    .replace(/^\w/, (c) => c.toUpperCase())
    .trim();
}

function _formatProjValue(key, val) {
  if (val === null || val === undefined || val === "" || val === "None")
    return "";
  const s = String(val);

  if (key === "status") {
    const txt = _getStatusText(val);
    if (txt === "On-going") return '<span class="status-badge badge-active">On-going</span>';
    if (txt === "Completed") return '<span class="status-badge badge-completed">Completed</span>';
    return txt;
  }

  // Billing type
  if (key === "billingType") {
    const map = {
      0: "Non-Billable",
      1: "Fixed Price",
      2: "T&M",
      3: "Retainer",
    };
    return map[s] || s;
  }

  // JSON arrays (project managers)
  try {
    const parsed = JSON.parse(s);
    if (Array.isArray(parsed)) {
      if (parsed.length === 0)
        return '<span style="color:var(--clr-text-muted)">None assigned</span>';
      const names = parsed.map((item) => {
        if (typeof item === "object") {
          const first = item.firstName || item.first_name || "";
          const last = item.lastName || item.last_name || "";
          return (
            item.name ||
            item.displayName ||
            `${first} ${last}`.trim() ||
            JSON.stringify(item)
          );
        }
        return String(item);
      });
      return names
        .map(
          (n) =>
            `<span class="status-badge badge-active" style="margin:2px">${_escP(n)}</span>`,
        )
        .join(" ");
    }
    if (typeof parsed === "object" && parsed !== null) {
      if (parsed.city || parsed.state || parsed.countryCode) {
        const parts = [
          parsed.addressLine1,
          parsed.addressLine2,
          parsed.city,
          parsed.state,
          parsed.zip,
          parsed.countryCode,
        ].filter(Boolean);
        if (parts.length > 0) return parts.join(", ");
      }
      return parsed.name || parsed.displayName || JSON.stringify(parsed);
    }
  } catch {
    /* not JSON */
  }

  // Dates
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    try {
      const d = new Date(s);
      if (!isNaN(d.getTime()) && d.getFullYear() > 1900) {
        return d.toLocaleDateString("en-GB", {
          day: "2-digit",
          month: "short",
          year: "numeric",
        });
      }
    } catch {
      /* fallback */
    }
    if (s.startsWith("0001-") || s.startsWith("1900-"))
      return '<span style="color:var(--clr-text-muted)">Not set</span>';
  }

  // Booleans
  if (s.toLowerCase() === "true")
    return '<span class="status-badge badge-true">Yes</span>';
  if (s.toLowerCase() === "false")
    return '<span class="status-badge badge-false">No</span>';

  // Budget
  if (key.toLowerCase().includes("budget")) {
    const num = parseFloat(s);
    if (!isNaN(num)) return `₹ ${num.toLocaleString("en-IN")}`;
  }

  return _escP(s);
}

// function _buildProjCard(title, icon, fields, data) {
//     const fieldsWithData = fields.filter(f => {
//         if (!(f in data)) return false;
//         return _formatProjValue(f, data[f]) !== '';
//     });
//     if (fieldsWithData.length === 0) return '';

//     const WIDE_FIELDS = new Set(['projectManagers']);

//     const fieldHTML = fieldsWithData.map(f => {
//         const isWide = WIDE_FIELDS.has(f);
//         const cls = isWide ? 'form-field form-field-wide' : 'form-field';
//         return `
//             <div class="${cls}">
//                 <label>${_formatProjLabel(f)}</label>
//                 <div class="field-value">${_formatProjValue(f, data[f])}</div>
//             </div>`;
//     }).join('');

//     return `
//         <div class="detail-card">
//             <div class="detail-card-header"><i class="bi ${icon}"></i> ${title}</div>
//             <div class="detail-card-body">${fieldHTML}</div>
//         </div>`;
// }
function _buildProjCard(title, icon, fields, data) {
  const fieldsWithData = fields.filter((f) => {
    if (!(f in data)) return false;
    return _formatProjValue(f, data[f]) !== "";
  });
  if (fieldsWithData.length === 0) return "";

  const WIDE_FIELDS = new Set(["projectManagers"]);

  const fieldHTML = fieldsWithData
    .map((f) => {
      const isWide = WIDE_FIELDS.has(f);
      const cls = isWide ? "form-field form-field-wide" : "form-field";
      return `
            <div class="${cls}">
                <label>${_formatProjLabel(f)}</label>
                <div class="field-value">${_formatProjValue(f, data[f])}</div>
            </div>`;
    })
    .join("");

  return `
        <div class="detail-card">
            <div class="detail-card-header"><i class="bi ${icon}"></i> ${title}</div>
            <div class="detail-card-body">${fieldHTML}</div>
        </div>`;
}

function _escP(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function _formatDateShort(iso) {
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleDateString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
    });
  } catch {
    return iso;
  }
}

/* ═══ Project Sub-Tab Switching ══════════════════════════ */

function switchProjTab(tab) {
  _activeProjSubTab = tab;

  document
    .querySelectorAll(".proj-sub-panel")
    .forEach((p) => (p.style.display = "none"));
  document
    .querySelectorAll(".proj-sub-tab")
    .forEach((b) => b.classList.remove("active"));

  if (tab === "overview") {
    document.getElementById("projDetailPanel").style.display = "";
    document.getElementById("projTabOverview").classList.add("active");
    const mgrWrap = document.getElementById("projManagerWrap");
    if (mgrWrap) mgrWrap.style.display = "flex";
  } else if (tab === "resources") {
    document.getElementById("projResourcesPanel").style.display = "";
    document.getElementById("projTabResources").classList.add("active");
    const mgrWrap = document.getElementById("projManagerWrap");
    if (mgrWrap) mgrWrap.style.display = "none";

    // Lazy-load: only fetch if panel still shows the placeholder empty-state
    const rPanel = document.getElementById("projResourcesPanel");
    if (rPanel && rPanel.querySelector(".empty-state")) {
      const proj = _projData[_currentProjIdx];
      if (proj) _fetchProjectResources(proj.id || "");
    }
  }
}

/* ═══ Project Resources Fetch + Render ══════════════════ */

async function _fetchProjectResources(projectId) {
  const rPanel = document.getElementById("projResourcesPanel");
  if (!rPanel) return;

  rPanel.innerHTML = `
        <div class="loading-spinner">
            <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
            Loading resources...
        </div>`;

  try {
    const url = projectId
      ? `/api/project/resources?projectId=${encodeURIComponent(projectId)}`
      : "/api/project/resources";
    const res = await fetch(url);
    const result = await res.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      rPanel.innerHTML = _buildResourceTable(result.data);
    } else {
      rPanel.innerHTML = `
                <div class="empty-state">
                    <i class="bi bi-people"></i>
                    <p>No resources found for this project.</p>
                </div>`;
    }
  } catch (err) {
    rPanel.innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle"></i>
                <p>Error loading resources: ${_escP(err.message)}</p>
            </div>`;
  }
}

// function _buildResourceTable(rows) {
//     const COLS = [
//         { key: 'employeeName', label: 'Employee Name' },
//         { key: 'projectName',  label: 'Project Name'  },
//         { key: 'name',         label: 'Allocation'    },
//         { key: 'startdate',    label: 'Start Date'    },
//         { key: 'enddate',      label: 'End Date'      },
//         { key: 'daysWorked',   label: 'Days Worked'   },
//     ];

//     const headerHTML = COLS.map(c => `<th>${c.label}</th>`).join('');

//     const rowsHTML = rows.map((r, i) => {
//         const cells = COLS.map(c => {
//             const val = r[c.key];

//             // Format date columns
//             // Days Worked logic
//             if (c.key === 'daysWorked') {
//             return `<td>${_calcDaysWorked(r.startdate, r.enddate)}</td>`;
//             }

//             // Default
//             return `<td>${val ? _escP(String(val)) : '—'}</td>`;
//         }).join('');

//         return `<tr class="${i % 2 === 0 ? 'row-even' : 'row-odd'}">${cells}</tr>`;
//     }).join('');

//     return `
//         <div class="proj-resource-wrap">
//             <div class="proj-resource-count">
//                 <i class="bi bi-people-fill"></i>
//                 ${rows.length} resource${rows.length !== 1 ? 's' : ''} allocated
//             </div>
//             <table class="proj-resource-table">
//                 <thead><tr>${headerHTML}</tr></thead>
//                 <tbody>${rowsHTML}</tbody>
//             </table>
//         </div>`;
// }
function _buildResourceTable(rows) {
  const COLS = [
    { key: "employeeName", label: "Employee Name" },
    { key: "projectName", label: "Project Name" },
    // { key: 'name',         label: 'Allocation'    },
    { key: "startdate", label: "Start Date" },
    { key: "enddate", label: "End Date" },
    { key: "daysWorked", label: "Logged Days" },
  ];

  // ✅ Center headers for specific columns
  const headerHTML = COLS.map((c) => {
    if (["projectName", "startdate", "enddate", "daysWorked"].includes(c.key)) {
      return `<th class="text-center">${c.label}</th>`;
    }
    return `<th>${c.label}</th>`;
  }).join("");

  const rowsHTML = rows
    .map((r, i) => {
      const cells = COLS.map((c) => {
        const val = r[c.key];

        // CENTER THESE
        if (["projectName", "name", "startdate", "enddate"].includes(c.key)) {
          return `<td class="text-center">${val ? _escP(String(val)) : "—"}</td>`;
        }

        if (c.key === "daysWorked") {
          const days = r.daysWorked != null ? r.daysWorked : 0;
          return `<td class="text-center">${days} days</td>`;
        }

        // DEFAULT
        return `<td>${val ? _escP(String(val)) : "—"}</td>`;
      }).join("");

      return `<tr class="${i % 2 === 0 ? "row-even" : "row-odd"}">${cells}</tr>`;
    })
    .join("");

  return `
        <div class="proj-resource-wrap">
            <div class="proj-resource-count">
                <i class="bi bi-people-fill"></i>
                ${rows.length} resource${rows.length !== 1 ? "s" : ""} allocated
            </div>
            <table class="proj-resource-table">
                <thead><tr>${headerHTML}</tr></thead>
                <tbody>${rowsHTML}</tbody>
            </table>
        </div>`;
}

/* ═══ Account Manager Selection ═════════════════════════ */

let _accountManagerNames = new Set();
let _selectedManagerName = "";

async function _fetchAccountManagers() {
  try {
    const response = await fetch("/api/employee/data");
    const result = await response.json();
    if (result.status === "success" && result.data) {
      const listEl = document.getElementById("projManagerDataList");
      let opts = [];
      result.data.forEach((emp) => {
        const name =
          emp.displayName ||
          `${emp.firstName || ""} ${emp.lastName || ""}`.trim();
        if (name) {
          _accountManagerNames.add(name);
          opts.push(`<option value="${_escP(name)}">`);
        }
      });
      listEl.innerHTML = opts.join("");
    }
  } catch (e) {
    console.error("Failed to load account managers for drop-down", e);
  }
}

function _setupAccountManagerEvents() {
  const mgrInput = document.getElementById("projManagerSelect");
  if (!mgrInput) return;

  mgrInput.addEventListener("change", () => {
    const val = mgrInput.value.trim();
    // Only trigger modal if value is non-empty and in the list
    if (val && _accountManagerNames.has(val)) {
      _selectedManagerName = val;
      document.getElementById("projManagerOverlay").style.display = "flex";
      document.getElementById("projManagerComment").value = "";
      document.getElementById("projManagerCommentError").style.display = "none";
    } else if (val) {
      // Invalid selection reset
      mgrInput.value = "";
    }
  });
}

function _cancelManagerModal() {
  document.getElementById("projManagerOverlay").style.display = "none";
  document.getElementById("projManagerSelect").value = "";
  _selectedManagerName = "";
}

async function _submitManagerModal() {
  const comments = document.getElementById("projManagerComment").value.trim();
  const errorEl = document.getElementById("projManagerCommentError");
  const submitBtn = document.getElementById("projManagerSubmitBtn");
  const mgrInput = document.getElementById("projManagerSelect");

  if (!comments) {
    errorEl.style.display = "block";
    return;
  }

  errorEl.style.display = "none";

  const proj = _projData[_currentProjIdx];
  if (!proj || !proj.id) return;

  // Show loading state and instantly hide modal for speedy UX
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = "Saving…";
  }
  document.getElementById("projManagerOverlay").style.display = "none";

  if (mgrInput) {
    mgrInput.disabled = true;
    mgrInput.value = ""; // Clear value so Chrome doesn't save text into autocomplete cache
    mgrInput.placeholder = "⏳ Saving…";
  }

  if (typeof _showToast !== "undefined") {
    _showToast("Saving Account Manager to database...", "success");
  }

  try {
    const payload = {
      projectId: proj.id,
      accountManager: _selectedManagerName,
      comments: comments,
    };

    const res = await fetch("/api/project/account-manager", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const result = await res.json();

    if (result.status === "success") {
      if (typeof _showToast !== "undefined") {
        _showToast("Account manager updated successfully!", "success");
      }

      // Wait for data to reload so it perfectly restores the current project
      await fetchProjectData();
    } else {
      console.error("Failed to update account manager:", result.message);
      if (typeof _showToast !== "undefined") {
        _showToast("Error: " + result.message, "error");
      }
    }
  } catch (e) {
    console.error("Failed API call for account-manager", e);
  } finally {
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.textContent = "Submit";
    }
    if (mgrInput) {
      mgrInput.disabled = false;
      mgrInput.placeholder = "Set Account Manager"; // Reset placeholder

      try {
        // If the fetch succeeded, _showProject() will have updated the UI already,
        // but we clear the input here just in case, since it's an autocomplete input.
        const currentMgr =
          (_projData[_currentProjIdx] || {}).accountmanager || "";
        mgrInput.value = currentMgr;
      } catch (e) {}
    }
  }
}

function _formatDateShort(dateStr) {
  if (
    !dateStr ||
    dateStr.toLowerCase() === "none" ||
    dateStr.toLowerCase() === "null"
  )
    return "—";
  const d = new Date(dateStr);
  if (isNaN(d)) return dateStr;
  const day = String(d.getDate()).padStart(2, "0");
  const m = d.toLocaleString("default", { month: "short" });
  const y = String(d.getFullYear()).slice(-2);
  return `${day}-${m}-${y}`;
}
