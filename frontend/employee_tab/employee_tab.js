/* ═══════════════════════════════════════════════════════════
   Employee Tab — Filter Dropdowns + Detail Cards + Sub-Tabs
   ═══════════════════════════════════════════════════════════ */

let _empData = [];
let _currentEmpIdx = 0;
let _activeEmpSubTab = "profile";

/* ── Fields to completely hide ──────────────────────────── */
const HIDDEN_FIELDS = new Set([
  "id",
  "uuid",
  "createdDate",
  "modifiedDate",
  "customAttributes",
  "profilePhotoUrl",
  "image",
  "holidayCalendarId",
  "trackingPolicyInfo",
  "attendanceNumber",
  "secondaryJobTitle",
  "professionalSummary",
  "workerType",
  "timeType",
  "middleName",
]);

/* ── JSON title extraction fields ───────────────────────── */
const JSON_TITLE_FIELDS = new Set([
  "expensePolicyInfo",
  "shiftPolicyInfo",
  "weeklyOffPolicyInfo",
  "reportsTo",
  "department",
  "businessUnit",
  "location",
  "jobTitle",
  "company",
  "legalEntity",
  "costCenter",
  "payGroup",
  "payBand",
  "payGrade",
  "l2Manager",
  "reportingManager",
  "secondaryReportingManager",
]);

/* ── Better field grouping with hierarchy ───────────────── */
const FIELD_GROUPS = [
  {
    title: "General Information",
    icon: "bi-person-fill",
    fields: [
      "firstName", "lastName", "displayName", "email",
      "employeeNumber", "designation", "jobTitle", "gender",
      "isprofilecomplete", "isProfileComplete"
    ],
  },
  {
    title: "Contact",
    icon: "bi-telephone-fill",
    fields: ["workPhone", "homePhone", "personalEmail", "mobilephone", "mobilePhone"],
  },
  {
    title: "Reporting Hierarchy",
    icon: "bi-diagram-3-fill",
    fields: ["reportsTo", "l2Manager", "reportingManager", "secondaryReportingManager"],
  },
  {
    title: "Organisation",
    icon: "bi-building",
    fields: ["department", "businessUnit", "location", "company", "legalEntity", "costCenter", "city", "countrycode", "countryCode"],
  },
  {
    title: "Employment & Leave",
    icon: "bi-briefcase-fill",
    fields: [
      "dateOfJoining", "joiningdate", "startdate",
      "probationEndDate", "confirmationDate",
      "employmentType", "noticePeriod", "totalExperienceInDays",
      "status", "employeeStatus", "employmentstatus", "contingenttype", "captureschemeinfo", "leaveplaninfo",
      "exitstatus", "exittype"
    ],
  },
  {
    title: "Groups & Policies",
    icon: "bi-shield-check",
    fields: ["groups", "expensePolicyInfo", "shiftPolicyInfo", "weeklyOffPolicyInfo", "payGroup", "payBand", "payGrade"],
  },
  {
    title: "Personal",
    icon: "bi-person-vcard",
    fields: ["nationality", "maritalStatus", "maritalstatus", "dateOfBirth", "relations", "bloodGroup", "bloodgroup", "isprivate"],
  },
];

/* ── Label overrides for clean display ──────────────────── */
const LABEL_OVERRIDES = {
  totalExperienceInDays: "Total Experience",
  employeeNumber: "Employee ID",
  displayName: "Display Name",
  dateOfJoining: "Date of Joining",
  probationEndDate: "Probation End Date",
  confirmationDate: "Confirmation Date",
  employmentType: "Employment Type",
  noticePeriod: "Notice Period (Days)",
  employeeStatus: "Status",
  expensePolicyInfo: "Expense Policy",
  shiftPolicyInfo: "Shift Policy",
  weeklyOffPolicyInfo: "Weekly Off Policy",
  reportsTo: "L1 Manager",
  l2Manager: "L2 Manager",
  reportingManager: "Reporting Manager",
  secondaryReportingManager: "Secondary Manager",
  businessUnit: "Business Unit",
  dateOfBirth: "Date of Birth",
  maritalStatus: "Marital Status",
  costCenter: "Cost Center",
  legalEntity: "Legal Entity",
  payGroup: "Pay Group",
  payBand: "Pay Band",
  payGrade: "Pay Grade",
  workPhone: "Work Phone",
  homePhone: "Home Phone",
  personalEmail: "Personal Email",
  jobTitle: "Job Title",
  firstName: "First Name",
  lastName: "Last Name",
  bloodGroup: "Blood Group",
  bloodgroup: "Blood Group",
  accountstatus: "Account Status",
  invitationstatus: "Invitation Status",
  employmentstatus: "Employment Status",
  exitstatus: "Exit Status",
  exittype: "Exit Type",
  contingenttype: "Contingent Type",
  captureschemeinfo: "Capture Scheme",
  leaveplaninfo: "Leave Plan",
  countrycode: "Country Code",
  joiningdate: "Joining Date",
  mobilephone: "Mobile Phone",
  isprivate: "Private Profile",
  isprofilecomplete: "Profile Complete",
  maritalstatus: "Marital Status",
};

/* ═══ Init ══════════════════════════════════════════════ */

document.addEventListener("DOMContentLoaded", () => {
  fetchEmployeeData();
  _initTimesheetDefaults();

  // Enable/Disable Amount based on Type
  const typeField   = document.getElementById("certFieldType");
  const amountField = document.getElementById("certFieldAmount");

  if (typeField && amountField) {
    typeField.addEventListener("change", () => {
      if (typeField.value === "Paid") {
        amountField.disabled = false;
        amountField.required = true;
      } else {
        amountField.disabled = true;
        amountField.required = false;
        amountField.value = "";
      }
    });
    // Initial state
    amountField.disabled = typeField.value !== "Paid";
  }
});

async function fetchEmployeeData() {
  try {
    const response = await fetch("/api/employee/data");
    const result   = await response.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      let previousId = null;
      if (_empData && _empData.length > 0 && _currentEmpIdx < _empData.length) {
        const e = _empData[_currentEmpIdx];
        previousId = e.employeeNumber || e.employee_code || "";
      }

      _empData = result.data;
      _buildFilters();

      const savedId = sessionStorage.getItem("pmox_restore_emp_id");

      if (savedId) {
        sessionStorage.removeItem("pmox_restore_emp_id");
        const restoredIdx = _empIdMap[savedId];
        if (restoredIdx !== undefined) {
          _syncFilters(restoredIdx);
          _showEmployee(restoredIdx);
        } else {
          _showEmployee(0);
        }
      } else if (previousId && _empIdMap[previousId] !== undefined) {
        const restoredIdx = _empIdMap[previousId];
        _syncFilters(restoredIdx);
        _showEmployee(restoredIdx);
      } else {
        _showEmployee(0);
      }
    } else {
      document.getElementById("empDetailPanel").innerHTML = `
        <div class="empty-state">
          <i class="bi bi-people"></i>
          <p>No employee data available</p>
        </div>`;
    }
  } catch (error) {
    console.error("Error:", error);
    document.getElementById("empDetailPanel").innerHTML = `
      <div class="empty-state">
        <i class="bi bi-exclamation-triangle"></i>
        <p>Error loading employee data</p>
      </div>`;
  }
}

/* ═══ Timesheet Date Defaults ═══════════════════════════ */

function _initTimesheetDefaults() {
  const today = new Date();
  const prior = new Date();
  prior.setDate(today.getDate() - 30);

  const fmt   = (d) => d.toISOString().split("T")[0];
  const fromEl = document.getElementById("tsFromDate");
  const toEl   = document.getElementById("tsToDate");
  if (fromEl) fromEl.value = fmt(prior);
  if (toEl)   toEl.value   = fmt(today);
}

/* ═══ Sub-Tab Switching ═════════════════════════════════ */

function switchEmpTab(tab) {
  _activeEmpSubTab = tab;
  const panels  = document.querySelectorAll(".emp-sub-panel");
  panels.forEach((p) => (p.style.display = "none"));

  const buttons = document.querySelectorAll(".emp-sub-tab");
  buttons.forEach((b) => b.classList.remove("active"));

  const statusWrap = document.getElementById("empStatusWrap");
  if (statusWrap) {
    statusWrap.style.display = tab === "profile" ? "flex" : "none";
  }

  if (tab === "profile") {
    document.getElementById("empDetailPanel").style.display = "";
    document.getElementById("empTabProfile").classList.add("active");
  } else if (tab === "timesheet") {
    document.getElementById("empTimesheetPanel").style.display = "";
    document.getElementById("empTabTimesheet").classList.add("active");
    const results = document.getElementById("empTimesheetResults");
    if (results && results.querySelector(".empty-state")) {
      triggerTimesheetFetch();
    }
  } else if (tab === "history") {
    document.getElementById("empHistoryPanel").style.display = "";
    document.getElementById("empTabHistory").classList.add("active");
    _fetchHistoryData();
  }
}

/* ═══ Status Dropdown & Modal ═══════════════════════════ */

let _pendingStatus = "";

function _initStatusDropdown() {
  const select = document.getElementById("empStatusSelect");
  if (!select) return;
  select.addEventListener("change", () => {
    _pendingStatus = select.value;
    select.value   = "";
    _openStatusModal(_pendingStatus);
  });
}

function _openStatusModal(status) {
  document.getElementById("empStatusModalTitle").textContent = `Set status: ${status}`;
  document.getElementById("empStatusComment").value          = "";
  document.getElementById("empStatusCommentError").style.display = "none";
  document.getElementById("empStatusModalOverlay").style.display = "flex";
  setTimeout(() => document.getElementById("empStatusComment").focus(), 50);
}

function _cancelStatusModal() {
  _pendingStatus = "";
  document.getElementById("empStatusModalOverlay").style.display = "none";
}

async function _submitStatusModal() {
  const comment = document.getElementById("empStatusComment").value.trim();
  if (!comment) {
    document.getElementById("empStatusCommentError").style.display = "flex";
    return;
  }

  const emp            = _empData[_currentEmpIdx];
  if (!emp) return;

  const employeeNumber = emp.employeeNumber || emp.id || "";
  const submitBtn      = document.getElementById("empStatusSubmitBtn");
  const statusSelect   = document.getElementById("empStatusSelect");

  submitBtn.disabled   = true;
  submitBtn.textContent = "Saving…";
  document.getElementById("empStatusModalOverlay").style.display = "none";

  if (statusSelect) {
    statusSelect.disabled = true;
    if (statusSelect.options.length > 0) {
      statusSelect.options[0].text = "⏳ Saving…";
    }
    statusSelect.value = "";
  }

  if (typeof _showToast === "function") {
    _showToast("Saving status to database...", "success");
  }

  try {
    const res    = await fetch("/api/employee/status", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ employeeNumber, status: _pendingStatus, comments: comment }),
    });
    const result = await res.json();

    if (result.status === "success") {
      _pendingStatus = "";
      if (typeof _showToast === "function") {
        _showToast("Status updated successfully!", "success");
      }
      await fetchEmployeeData();
    } else {
      document.getElementById("empStatusCommentError").textContent =
        `⚠️ ${result.message || "Update failed. Please try again."}`;
      document.getElementById("empStatusCommentError").style.display = "flex";
    }
  } catch (err) {
    document.getElementById("empStatusCommentError").textContent =
      `⚠️ Network error: ${err.message}`;
    document.getElementById("empStatusCommentError").style.display = "flex";
  } finally {
    submitBtn.disabled    = false;
    submitBtn.textContent = "Submit";

    if (statusSelect) {
      statusSelect.disabled = false;
      if (statusSelect.options.length > 0) {
        statusSelect.options[0].text = "Set Status";
      }
      try {
        const currentStatus = (_empData[_currentEmpIdx] || {}).status || "";
        statusSelect.value = currentStatus;
      } catch (e) {}
    }
  }
}

document.addEventListener("DOMContentLoaded", () => {
  _initStatusDropdown();

  const statusOverlay = document.getElementById("empStatusModalOverlay");
  if (statusOverlay) {
    statusOverlay.addEventListener("click", (e) => {
      if (e.target === statusOverlay) _cancelStatusModal();
    });
  }

  const certOverlay = document.getElementById("certModalOverlay");
  if (certOverlay) {
    certOverlay.addEventListener("click", (e) => {
      if (e.target === certOverlay) _closeCertModal();
    });
  }
});

/* ═══ Timesheet ════════════════════════════════════════ */

function triggerTimesheetFetch() {
  const emp = _empData[_currentEmpIdx];
  if (!emp) return;

  const from = document.getElementById("tsFromDate").value;
  const to   = document.getElementById("tsToDate").value;
  if (!from || !to) {
    _setTimesheetContent(
      '<div class="empty-state"><i class="bi bi-exclamation-circle"></i><p>Please select both From and To dates.</p></div>',
    );
    return;
  }

  const empId = emp.employeeIdentifier || emp.id || emp.employeeNumber || "";
  _fetchTimesheetData(empId, from, to);
}

async function _fetchTimesheetData(employeeId, from, to) {
  _setTimesheetContent(`
    <div class="loading-spinner">
      <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
      Fetching timesheet data...
    </div>`);

  try {
    const url    = `/api/employee/timesheet?employeeId=${encodeURIComponent(employeeId)}&from=${from}&to=${to}`;
    const res    = await fetch(url);
    const result = await res.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      _setTimesheetContent(_buildTimesheetTable(result.data));
    } else if (result.status === "success") {
      _setTimesheetContent(`
        <div class="empty-state">
          <i class="bi bi-calendar-x"></i>
          <p>No timesheet records found for the selected period.</p>
        </div>`);
    } else {
      _setTimesheetContent(`
        <div class="empty-state">
          <i class="bi bi-exclamation-triangle"></i>
          <p>${_esc(result.message || "Error fetching timesheet data.")}</p>
        </div>`);
    }
  } catch (err) {
    console.error("Timesheet fetch error:", err);
    _setTimesheetContent(`
      <div class="empty-state">
        <i class="bi bi-wifi-off"></i>
        <p>Network error: ${_esc(err.message)}</p>
      </div>`);
  }
}

function _setTimesheetContent(html) {
  const el = document.getElementById("empTimesheetResults");
  if (el) el.innerHTML = html;
}

function _buildTimesheetTable(records) {
  const rows = records
    .map((r) => {
      const date     = r.date ? _formatDateCompact(r.date) : "—";
      const duration = r.hoursFormatted || (r.totalMinutes != null ? `${r.totalMinutes} min` : "—");
      const project  = r.projectName
        ? _esc(r.projectName)
        : r.projectId
          ? `<span class="ts-id-chip">${_esc(r.projectId)}</span>`
          : "—";
      const task     = r.taskName
        ? _esc(r.taskName)
        : r.taskId
          ? `<span class="ts-id-chip">${_esc(r.taskId)}</span>`
          : "—";
      const billable = r.isBillable === true || r.isBillable === "true"
        ? '<span class="status-badge badge-active">Yes</span>'
        : '<span class="status-badge badge-inactive">No</span>';
      const status   = _timesheetStatusBadge(r.statusLabel || r.status);

      return `
        <tr>
          <td class="ts-date-cell text-start">${date}</td>
          <td class="ts-dur-cell text-center"><strong>${duration}</strong></td>
          <td class="ts-proj-cell text-center">${project}</td>
          <td class="ts-task-cell text-center">${task}</td>
          <td class="ts-center-cell text-center">${billable}</td>
          <td class="ts-center-cell text-center">${status}</td>
        </tr>`;
    })
    .join("");

  return `
    <div class="ts-summary-row">
      <i class="bi bi-list-check"></i> <strong>${records.length}</strong> time entr${records.length !== 1 ? "ies" : "y"}
    </div>
    <div class="ts-table-wrap">
      <table class="emp-timesheet-table">
        <thead>
          <tr>
            <th class="ts-th-date text-start">Date</th>
            <th class="ts-th-dur text-center"><i class="bi bi-clock"></i> Duration</th>
            <th class="text-center"><i class="bi bi-folder2-open"></i> Project</th>
            <th class="text-center"><i class="bi bi-check2-square"></i> Task</th>
            <th class="ts-th-center text-center">Billable</th>
            <th class="ts-th-center text-center">Status</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function _timesheetStatusBadge(status) {
  const s = String(status || "").toLowerCase();
  if (s === "approved")  return '<span class="status-badge badge-active">Approved</span>';
  if (s === "submitted") return '<span class="status-badge badge-running">Submitted</span>';
  if (s === "rejected")  return '<span class="status-badge badge-inactive">Rejected</span>';
  if (s === "draft")     return '<span class="status-badge badge-weekoff">Draft</span>';
  return status ? `<span class="status-badge">${_esc(String(status))}</span>` : "—";
}

function _formatDateCompact(iso) {
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    const day   = String(d.getDate()).padStart(2, "0");
    const month = d.toLocaleString("en-GB", { month: "short" });
    const year  = d.getFullYear();
    return `${day} ${month} ${year}`;
  } catch {
    return iso;
  }
}

function _formatDateShort(iso) {
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
  } catch {
    return iso;
  }
}

function _formatTime(iso) {
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", hour12: true });
  } catch {
    return iso;
  }
}

/* ═══ History ══════════════════════════════════════════ */

async function _fetchHistoryData() {
  const emp = _empData[_currentEmpIdx];
  if (!emp) return;

  const empNumber = emp.employeeNumber || emp.employee_code || "";
  _fetchStatusHistory(empNumber);
  _fetchProjectHistory(empNumber);
}

async function _fetchStatusHistory(empNumber) {
  const container = document.getElementById("empStatusHistoryContent");
  container.innerHTML = `
    <div class="loading-spinner">
      <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
      Fetching status history...
    </div>`;

  try {
    const res    = await fetch(`/api/employee/history?employeeId=${encodeURIComponent(empNumber)}`);
    const result = await res.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      container.innerHTML = _buildStatusHistoryTable(result.data);
    } else if (result.status === "success") {
      container.innerHTML = `
        <div class="empty-state">
          <i class="bi bi-journal-x"></i>
          <p>No status history found for this employee.</p>
        </div>`;
    } else {
      container.innerHTML = `
        <div class="empty-state">
          <i class="bi bi-exclamation-triangle"></i>
          <p>${_esc(result.message || "Error fetching status history.")}</p>
        </div>`;
    }
  } catch (err) {
    container.innerHTML = `
      <div class="empty-state">
        <i class="bi bi-wifi-off"></i>
        <p>Network error: ${_esc(err.message)}</p>
      </div>`;
  }
}

function _buildStatusHistoryTable(entries) {
  const totalChanges = entries.filter((e) => e.diffs && e.diffs.length > 0).length;

  const rows = entries
    .map((entry) => {
      const isCurrent   = entry.is_current;
      const startLabel  = entry.startdate ? _formatDateShort(entry.startdate) : "—";
      const endLabel    = isCurrent || !entry.enddate || entry.enddate === "null"
        ? '<span class="hist-badge-current">Current</span>'
        : _formatDateShort(entry.enddate);

      const statusBadge = entry.status
        ? `<span class="hist-status-badge hist-status-${_slugify(entry.status)}">${_esc(entry.status)}</span>`
        : '<span class="sh-empty">—</span>';

      const commentVal  = entry.comments
        ? _esc(entry.comments)
        : '<span class="sh-empty">—</span>';

      let changeCell = "";
      if (entry.version === 1) {
        changeCell = `<span class="sh-init-pill"><i class="bi bi-bookmark-star"></i> Record created</span>`;
      } else if (entry.diffs && entry.diffs.length > 0) {
        changeCell = entry.diffs
          .map((d) => `
            <div class="sh-diff-row">
              <span class="sh-diff-field">${_esc(d.field)}</span>
              <span class="sh-diff-before">${d.before ? _esc(d.before) : "<em>—</em>"}</span>
              <i class="bi bi-arrow-right sh-diff-arrow"></i>
              <span class="sh-diff-after">${d.after ? _esc(d.after) : "<em>—</em>"}</span>
            </div>`)
          .join("");
      } else {
        changeCell = `<span class="sh-empty"><i class="bi bi-check2-circle"></i> No changes</span>`;
      }

      const rowClass = isCurrent ? "sh-row-current" : "";

      return `
        <tr class="${rowClass}">
          <td class="sh-td-period">
            <span class="sh-date">${startLabel}</span>
            <span class="sh-date-sep">→</span>
            <span class="sh-date">${endLabel}</span>
          </td>
          <td class="sh-td-status">${statusBadge}</td>
          <td class="sh-td-comment">${commentVal}</td>
          <td class="sh-td-changes">${changeCell}</td>
        </tr>`;
    })
    .join("");

  return `
    <div class="sh-meta">
      <i class="bi bi-clock-history"></i>
      <strong>${entries.length}</strong> version${entries.length !== 1 ? "s" : ""} &nbsp;·&nbsp;
      <strong>${totalChanges}</strong> change event${totalChanges !== 1 ? "s" : ""}
    </div>
    <div class="sh-table-wrap">
      <table class="sh-table">
        <thead>
          <tr>
            <th class="sh-th-period"><i class="bi bi-calendar2-event"></i> Period</th>
            <th class="sh-th-status">PMO Status</th>
            <th class="sh-th-comment">PMO Comments</th>
            <th class="sh-th-changes">Field Changes</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

async function _fetchProjectHistory(empNumber) {
  const container = document.getElementById("empProjectHistoryContent");
  container.innerHTML = `
    <div class="loading-spinner">
      <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
      Fetching project history...
    </div>`;

  try {
    const res    = await fetch(`/api/employee/project-history?employeeId=${encodeURIComponent(empNumber)}`);
    const result = await res.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      container.innerHTML = _buildProjectHistoryTable(result.data);
    } else if (result.status === "success") {
      container.innerHTML = `
        <div class="empty-state">
          <i class="bi bi-folder-x"></i>
          <p>No project history found for this employee.</p>
        </div>`;
    } else {
      container.innerHTML = `
        <div class="empty-state">
          <i class="bi bi-exclamation-triangle"></i>
          <p>${_esc(result.message || "Error fetching project history.")}</p>
        </div>`;
    }
  } catch (err) {
    container.innerHTML = `
      <div class="empty-state">
        <i class="bi bi-wifi-off"></i>
        <p>Network error: ${_esc(err.message)}</p>
      </div>`;
  }
}

function _buildProjectHistoryTable(records) {
  const rows = records
    .map((r) => {
      const projectName = r.projectName ? _esc(r.projectName) : '<span class="sh-empty">—</span>';
      const projectCode = r.projectCode ? _esc(r.projectCode) : '<span class="sh-empty">—</span>';
      const startDate   = r.startdate   ? _formatDateShort(r.startdate)  : "—";
      const endDate     = r.enddate     ? _formatDateShort(r.enddate)    : "—";
      const daysWorked  = r.days_worked  != null ? `${r.days_worked} days`  : '<span class="sh-empty">—</span>';
      const hoursWorked = r.hours_worked != null ? `${r.hours_worked} hrs`  : '<span class="sh-empty">—</span>';

      const isActive = r.project_status
        ? r.project_status === "Active"
        : (!r.enddate || ["null", "none", ""].includes(String(r.enddate).trim().toLowerCase()));

      const statusBadge = isActive
        ? '<span class="status-badge badge-active">Active</span>'
        : '<span class="status-badge badge-inactive">Inactive</span>';

      return `
        <tr>
          <td class="ph-td-name" style="white-space:normal;word-break:break-word;max-width:250px;">${projectName}</td>
          <td class="ph-td-code text-center">${projectCode}</td>
          <td class="ph-td-date text-center">${startDate}</td>
          <td class="ph-td-date text-center">${endDate}</td>
          <td class="ph-td-days text-center">${daysWorked}</td>
          <td class="ph-td-hours text-center">${hoursWorked}</td>
          <td class="ph-td-status text-center">${statusBadge}</td>
        </tr>`;
    })
    .join("");

  const activeCount = records.filter(
    (r) => r.project_status
      ? r.project_status === "Active"
      : (!r.enddate || ["null", "none", ""].includes(String(r.enddate || "").trim().toLowerCase()))
  ).length;

  return `
    <div class="sh-meta">
      <i class="bi bi-folder2-open"></i>
      <strong>${records.length}</strong> project assignment${records.length !== 1 ? "s" : ""} &nbsp;·&nbsp;
      <strong>${activeCount}</strong> active
    </div>
    <div class="sh-table-wrap">
      <table class="sh-table">
        <thead>
          <tr>
            <th style="width:32%;"><i class="bi bi-folder2"></i> Project Name</th>
            <th class="text-center" style="width:15%;">Project Code</th>
            <th class="text-center" style="width:13%;"><i class="bi bi-calendar-event"></i> Start Date</th>
            <th class="text-center" style="width:13%;"><i class="bi bi-calendar-check"></i> End Date</th>
            <th class="text-center" style="width:10%;">Logged Days</th>
            <th class="text-center" style="width:10%;">Total Hours</th>
            <th class="text-center" style="width:7%;">Status</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function _slugify(str) {
  return (str || "").toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "");
}

/* ═══ Searchable Filter Inputs ═════════════════════════ */

let _empNameMap  = {};
let _empIdMap    = {};
let _empEmailMap = {};

function _buildFilters() {
  const nameInput  = document.getElementById("empFilterName");
  const idInput    = document.getElementById("empFilterId");
  const emailInput = document.getElementById("empFilterEmail");

  const nameList  = document.getElementById("empNameList");
  const idList    = document.getElementById("empIdList");
  const emailList = document.getElementById("empEmailList");

  _empNameMap  = {};
  _empIdMap    = {};
  _empEmailMap = {};

  const nameOpts  = [];
  const idOpts    = [];
  const emailOpts = [];

  const nameCounts = {};
  _empData.forEach((emp) => {
    const name = _empName(emp);
    nameCounts[name] = (nameCounts[name] || 0) + 1;
  });

  _empData.forEach((emp, i) => {
    const rawName = _empName(emp);
    const id      = emp.employeeNumber || emp.employee_code || "";
    const email   = emp.email || "";

    const resolvedName = (nameCounts[rawName] > 1 && id) ? `${rawName} (${id})` : rawName;

    _empNameMap[resolvedName] = i;
    if (id)    _empIdMap[id]       = i;
    if (email) _empEmailMap[email] = i;

    nameOpts.push(resolvedName);
    if (id)    idOpts.push(id);
    if (email) emailOpts.push(email);
  });

  nameOpts.sort();
  idOpts.sort();
  emailOpts.sort();

  nameList.innerHTML  = nameOpts.map((v)  => `<option value="${_esc(v)}">`).join("");
  idList.innerHTML    = idOpts.map((v)    => `<option value="${_esc(v)}">`).join("");
  emailList.innerHTML = emailOpts.map((v) => `<option value="${_esc(v)}">`).join("");

  const firstEmp = _empData[0];
  nameInput.value  = _empName(firstEmp);
  idInput.value    = firstEmp.employeeNumber || firstEmp.employee_code || "";
  emailInput.value = firstEmp.email || "";

  nameInput.addEventListener("change", () => {
    const idx = _empNameMap[nameInput.value];
    if (idx !== undefined) { _syncFilters(idx); _showEmployee(idx); }
  });
  idInput.addEventListener("change", () => {
    const idx = _empIdMap[idInput.value];
    if (idx !== undefined) { _syncFilters(idx); _showEmployee(idx); }
  });
  emailInput.addEventListener("change", () => {
    const idx = _empEmailMap[emailInput.value];
    if (idx !== undefined) { _syncFilters(idx); _showEmployee(idx); }
  });
}

function _syncFilters(idx) {
  const emp = _empData[idx];
  document.getElementById("empFilterName").value  = _empName(emp);
  document.getElementById("empFilterId").value    = emp.employeeNumber || emp.employee_code || "";
  document.getElementById("empFilterEmail").value = emp.email || "";
}

/* ═══ Render Employee Detail ═══════════════════════════ */

function _showEmployee(idx) {
  _currentEmpIdx = idx;

  switchEmpTab("profile");

  const tsResults = document.getElementById("empTimesheetResults");
  if (tsResults) {
    tsResults.innerHTML = `
      <div class="empty-state">
        <i class="bi bi-clock"></i>
        <p>Select a date range and click Fetch to load timesheet data.</p>
      </div>`;
  }

  const statusHistContent = document.getElementById("empStatusHistoryContent");
  if (statusHistContent) {
    statusHistContent.innerHTML = `<div class="empty-state"><i class="bi bi-journal-text"></i><p>Loading status history...</p></div>`;
  }
  const projHistContent = document.getElementById("empProjectHistoryContent");
  if (projHistContent) {
    projHistContent.innerHTML = `<div class="empty-state"><i class="bi bi-folder"></i><p>Loading project history...</p></div>`;
  }

  _initTimesheetDefaults();

  const statusSelect = document.getElementById("empStatusSelect");
  if (statusSelect) {
    const currentStatus = (_empData[idx] || {}).status || "";
    statusSelect.value = currentStatus;
  }

  const emp   = _empData[idx];
  const panel = document.getElementById("empDetailPanel");

  const _pick = (...keys) =>
    keys.map(k => emp[k]).find(v => v != null && v !== "" && v !== "None" && v !== "null") ?? "";

  function _fmtDate(v) {
    if (!v || v === "None" || v === "null") return "";
    const s = String(v);
    if (s.startsWith("0001-") || s.startsWith("1900-")) return "";
    try {
      const d = new Date(s);
      if (!isNaN(d.getTime()) && d.getFullYear() > 1900)
        return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
    } catch {}
    return s;
  }

  function _fmtObj(v) {
    if (!v || v === "None" || v === "null") return "";
    const s = String(v).trim();
    if (!s.startsWith("{") && !s.startsWith("[")) return s;
    try {
      const obj = JSON.parse(s);
      if (Array.isArray(obj)) {
        return obj.map(item => {
          if (typeof item !== "object") return String(item);
          const first = item.firstName || item.first_name || "";
          const last  = item.lastName  || item.last_name  || "";
          return item.displayName || item.name || `${first} ${last}`.trim() || item.title || item.email || "";
        }).filter(Boolean).join(", ");
      }
      if (typeof obj === "object") {
        const first = obj.firstName || obj.first_name || "";
        const last  = obj.lastName  || obj.last_name  || "";
        return obj.displayName || obj.name || `${first} ${last}`.trim() || obj.title || obj.email || "";
      }
    } catch {}
    return s;
  }

  const _GENDER  = { "0": "", "1": "Male", "2": "Female", "3": "Other" };
  const _MARITAL = { "0": "", "1": "Single", "2": "Married", "3": "Divorced", "4": "Widowed" };
  const _BLOOD   = { "0": "", "1": "A+", "2": "A-", "3": "B+", "4": "B-", "5": "AB+", "6": "AB-", "7": "O+", "8": "O-" };
  const _decode  = (map, v) => map[String(v)] ?? (v === "None" ? "" : String(v));

  function _fmtExp(days) {
    const d = parseInt(days);
    if (!d || isNaN(d)) return "";
    const months = Math.floor(d / 30);
    if (months < 1)  return `${d} days`;
    if (months < 12) return `${months} mos`;
    const yr = Math.floor(months / 12), mo = months % 12;
    return mo > 0 ? `${yr} yr ${mo} mos` : `${yr} yrs`;
  }

  function _flatSec(heading, icon, pairs) {
    const cells = pairs
      .map(([lbl, v]) => {
        const display = (v == null || v === "" || v === "None" || v === "null") ? null : String(v).trim();
        if (!display) return "";
        return `<div class="flat-field">
          <span class="flat-lbl">${_esc(lbl)}</span>
          <span class="flat-val">${_esc(display)}</span>
        </div>`;
      }).join("");
    if (!cells.trim()) return "";
    return `<div class="flat-section">
      <div class="flat-section-hdr"><i class="bi ${icon}"></i> ${heading}</div>
      <div class="flat-grid">${cells}</div>
    </div>`;
  }

  const empNumber = emp.employeeNumber || emp.employee_code || "";

  const html = `
    ${_flatSec("Profile", "bi-person-fill", [
      ["First Name",       _pick("firstName", "first_name")],
      ["Last Name",        _pick("lastName", "last_name")],
      ["Display Name",     _pick("displayName", "display_name")],
      ["Email",            _pick("email")],
      ["Employee ID",      _pick("employeeNumber", "employee_code")],
      ["Job Title",        _fmtObj(_pick("jobTitle", "designation"))],
      ["Gender",           _decode(_GENDER, _pick("gender"))],
      ["Personal Email",   _pick("personalEmail")],
      ["Mobile Phone",     _pick("mobilePhone", "mobilephone")],
      ["Nationality",      _pick("nationality")],
      ["Marital Status",   _decode(_MARITAL, _pick("maritalStatus"))],
      ["Date of Birth",    _fmtDate(_pick("dateOfBirth"))],
      ["Blood Group",      _decode(_BLOOD, _pick("bloodGroup"))],
    ])}

    ${_flatSec("Work Details", "bi-briefcase-fill", [
      ["L1 Manager",         _fmtObj(_pick("reportsTo", "reportingManager"))],
      ["L2 Manager",         _fmtObj(_pick("l2Manager", "secondaryReportingManager"))],
      ["Department",         _pick("department")],
      ["Business Unit",      _pick("businessUnit")],
      ["City",               _pick("city", "location")],
      ["Country",            _pick("countryCode", "countrycode")],
      ["Start Date",         _fmtDate(_pick("dateOfJoining", "joiningdate", "startdate"))],
      ["Probation End Date", _fmtDate(_pick("probationEndDate"))],
      ["Employment Type",    _pick("employmentType")],
      ["Notice Period",      _pick("noticePeriod")],
      ["Total Experience",   _fmtExp(_pick("totalExperienceInDays"))],
      ["Status",             _pick("status", "employeeStatus", "employmentstatus")],
      ["Company",            _pick("company", "legalEntity")],
      ["Cost Center",        _pick("costCenter")],
    ])}

    <!-- Skills injected async -->
    <div id="empProfileSkillsBlock">
      <div class="skill-inline-loading">
        <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
        Loading skills...
      </div>
    </div>
  `;

  panel.innerHTML = html;
  _injectProfileSkills(empNumber);
  _injectProfileCertifications(empNumber);
}

/* ═══ Helpers ══════════════════════════════════════════ */

function _empName(emp) {
  const first   = emp.firstName   || emp.first_name   || "";
  const last    = emp.lastName    || emp.last_name    || "";
  const display = emp.displayName || emp.display_name || "";
  return display || `${first} ${last}`.trim() || emp.email || "Unknown";
}

function _extractTitle(val) {
  if (!val || val === "None") return "";
  if (typeof val === "string") {
    try { val = JSON.parse(val); } catch { return val; }
  }
  if (Array.isArray(val)) {
    return val.map((item) => _extractSingle(item)).filter(Boolean).join(", ");
  }
  return _extractSingle(val);
}

function _extractSingle(item) {
  if (!item || typeof item !== "object") return String(item || "");
  return (
    item.title ||
    item.name  ||
    item.displayName ||
    (item.firstName ? `${item.firstName} ${item.lastName || ""}`.trim() : "") ||
    ""
  );
}

function _formatLabel(key) {
  if (LABEL_OVERRIDES[key]) return LABEL_OVERRIDES[key];
  return key
    .replace(/([A-Z])/g, " $1")
    .replace(/_/g, " ")
    .replace(/^\w/, (c) => c.toUpperCase())
    .trim();
}

function _formatValue(key, val) {
  if (val === null || val === undefined || val === "" || val === "None") return "";
  const s = String(val);

  if (JSON_TITLE_FIELDS.has(key)) {
    const extracted = _extractTitle(val);
    if (extracted) return _esc(extracted);
    return "";
  }

  if (key === "groups") {
    try {
      let parsed = typeof val === "string" ? JSON.parse(val) : val;
      if (Array.isArray(parsed)) {
        const titles = parsed.map((g) => g.title || g.name || "").filter(Boolean);
        if (titles.length === 0) return "";
        return titles.map((t) => `<span class="status-badge badge-active" style="margin:2px">${_esc(t)}</span>`).join(" ");
      }
    } catch {}
    return _esc(s);
  }

  if (key === "relations") {
    const extracted = _extractTitle(val);
    if (extracted) return _esc(extracted);
    if (s && s !== "{}" && s !== "[]" && s !== "None") return _esc(s);
    return "";
  }

  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    try {
      const d = new Date(s);
      if (!isNaN(d.getTime()) && d.getFullYear() > 1900) {
        return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
      }
    } catch {}
    if (s.startsWith("0001-") || s.startsWith("1900-")) return "";
  }

  if (key === "totalExperienceInDays") {
    const days = parseInt(s, 10);
    if (!isNaN(days) && days > 0) {
      const years  = Math.floor(days / 365);
      const months = Math.floor((days % 365) / 30);
      let parts    = [];
      if (years  > 0) parts.push(`${years} yr${years   > 1 ? "s" : ""}`);
      if (months > 0) parts.push(`${months} mo${months > 1 ? "s" : ""}`);
      return parts.join(" ") || `${days} days`;
    }
  }

  if (s.toLowerCase() === "true")  return '<span class="status-badge badge-true">Yes</span>';
  if (s.toLowerCase() === "false") return '<span class="status-badge badge-false">No</span>';

  const lower = s.toLowerCase();
  if (lower === "active")   return '<span class="status-badge badge-active">Active</span>';
  if (lower === "inactive") return '<span class="status-badge badge-inactive">Inactive</span>';

  const cleanKey = key.toLowerCase().replace(/_/g, "").replace(/ /g, "");

  if (cleanKey === "gender") {
    const gmap = { 0: "Not specified", 1: "Male", 2: "Female", 3: "Other" };
    if (gmap[s]) return gmap[s];
  }
  if (cleanKey === "maritalstatus") {
    const msmap = { 0: "Not specified", 1: "Single", 2: "Married", 3: "Divorced", 4: "Widowed" };
    if (msmap[s]) return msmap[s];
  }
  if (cleanKey === "bloodgroup") {
    const bgmap = { 0: "Not Specified", 1: "A+", 2: "A-", 3: "B+", 4: "B-", 5: "AB+", 6: "AB-", 7: "O+", 8: "O-", 9: "A1+", 10: "A1-", 11: "A2+", 12: "A2-", 13: "A1B+", 14: "A1B-", 15: "A2B+", 16: "A2B-", 17: "Oh (Bombay Blood Group)" };
    if (bgmap[s]) return bgmap[s];
  }
  if (cleanKey === "accountstatus") {
    const asmap = { 0: "Inactive", 1: "Active", 2: "Suspended" };
    if (asmap[s]) return asmap[s];
  }
  if (cleanKey === "invitationstatus") {
    const ismap = { 0: "Not Invited", 1: "Invited", 2: "Accepted", 3: "Expired" };
    if (ismap[s]) return ismap[s];
  }
  if (cleanKey === "employmentstatus") {
    const esmap = { 0: "Active", 1: "In-Notice", 2: "Exited" };
    if (esmap[s]) return esmap[s];
  }
  if (cleanKey === "exitstatus" || cleanKey === "exittype") {
    if (s === "0") return "N/A";
  }
  if (cleanKey === "isprivate" || cleanKey === "isprofilecomplete") {
    if (s === "0" || s.toLowerCase() === "false") return '<span class="status-badge badge-false">No</span>';
    return '<span class="status-badge badge-true">Yes</span>';
  }
  if (cleanKey === "nationality" || cleanKey === "countrycode") {
    const nmap  = { IN: "Indian", US: "American", GB: "British", CA: "Canadian", AU: "Australian", SG: "Singaporean", AE: "UAE" };
    const lookup = s.toUpperCase();
    if (nmap[lookup]) return nmap[lookup];
    return lookup;
  }
  if (cleanKey === "employmenttype") {
    const map = { 0: "Full-time", 1: "Part-time", 2: "Contract", 3: "Intern", 4: "Freelancer" };
    if (map[s]) return map[s];
  }

  try {
    const parsed = JSON.parse(s);
    if (typeof parsed === "object" && parsed !== null) {
      const extracted = _extractTitle(parsed);
      if (extracted) return _esc(extracted);
      return "";
    }
  } catch {}

  return _esc(s);
}

function _buildCard(title, icon, fields, data) {
  const fieldsWithData = fields.filter((f) => {
    if (!(f in data)) return false;
    const formatted = _formatValue(f, data[f]);
    return formatted !== "";
  });
  if (fieldsWithData.length === 0) return "";

  const WIDE_FIELDS = new Set(["groups"]);

  const fieldHTML = fieldsWithData
    .map((f) => {
      const isWide = WIDE_FIELDS.has(f);
      const cls    = isWide ? "form-field form-field-wide" : "form-field";
      return `
        <div class="${cls}">
          <label>${_formatLabel(f)}</label>
          <div class="field-value">${_formatValue(f, data[f])}</div>
        </div>`;
    })
    .join("");

  return `
    <div class="detail-card">
      <div class="detail-card-header"><i class="bi ${icon}"></i> ${title}</div>
      <div class="detail-card-body">${fieldHTML}</div>
    </div>`;
}

function _esc(str) {
  const div       = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

/* ═══ Skill Matrix ══════════════════════════════════════ */

async function _injectProfileSkills(empNumber) {
  const block = document.getElementById("empProfileSkillsBlock");
  if (!block || !empNumber) return;

  try {
    const res    = await fetch(`/api/employee/skills?employeeId=${encodeURIComponent(empNumber)}`);
    const result = await res.json();

    if (result.status === "success" && result.data && result.data.length > 0) {
      block.innerHTML = _buildProfileSkillCard(result.data);
    } else if (result.status === "success") {
      block.style.display = "none";
    } else {
      block.innerHTML = `
        <div class="detail-card">
          <div class="detail-card-header"><i class="bi bi-stars"></i> Skills &amp; Expertise</div>
          <div style="padding:16px;color:#dc3545;font-size:0.82rem;">
            <i class="bi bi-exclamation-triangle"></i>
            Could not load skills: ${_esc(result.message || "Unknown error")}
          </div>
        </div>`;
    }
  } catch (err) {
    block.innerHTML = `
      <div class="detail-card">
        <div class="detail-card-header"><i class="bi bi-stars"></i> Skills &amp; Expertise</div>
        <div style="padding:16px;color:#dc3545;font-size:0.82rem;">
          <i class="bi bi-wifi-off"></i> Skills fetch error: ${_esc(err.message)}
        </div>
      </div>`;
  }
}

function _buildProfileSkillCard(records) {
  const groups = {};
  for (const skill of records) {
    const cat = skill.category || skill.skillCategory || "General";
    if (!groups[cat]) groups[cat] = [];
    groups[cat].push(skill);
  }

  const categoryHTML = Object.entries(groups)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([cat, skills]) => {
      const chips = skills.map((s) => {
        const name      = _esc(s.skillName || s.name || "—");
        const profRaw   = s.proficiencyLevel || s.proficiency || "";
        const profLabel = _skillProficiencyLabel(profRaw);
        const profClass = _skillProficiencyClass(profRaw);
        const expRaw    = s.experienceInYears || s.experience || s.experienceYears || "";
        const expLabel  = expRaw ? ` · ${parseFloat(expRaw).toFixed(1)} yrs` : "";

        return `
          <div class="skill-inline-chip">
            <span class="skill-inline-name">${name}</span>
            ${profLabel ? `<span class="skill-badge ${profClass}">${_esc(profLabel)}</span>` : ""}
            ${expLabel  ? `<span class="skill-inline-exp">${_esc(expLabel)}</span>`          : ""}
          </div>`;
      }).join("");

      return `
        <div class="skill-inline-category">
          <div class="skill-inline-cat-label">
            <i class="bi bi-tag-fill"></i> ${_esc(cat)}
          </div>
          <div class="skill-inline-chips">${chips}</div>
        </div>`;
    })
    .join("");

  return `
    <div class="detail-card">
      <div class="detail-card-header">
        <i class="bi bi-stars"></i> Skills &amp; Expertise
        <span class="skill-count-chip" style="margin-left:auto">${records.length} skill${records.length !== 1 ? "s" : ""}</span>
      </div>
      <div class="skill-inline-body">${categoryHTML}</div>
    </div>`;
}

function _skillProficiencyLabel(raw) {
  const num = parseInt(raw, 10);
  const map = { 0: "Beginner", 1: "Elementary", 2: "Intermediate", 3: "Advanced", 4: "Expert" };
  if (!isNaN(num) && map[num]) return map[num];
  return raw ? String(raw) : "";
}

function _skillProficiencyClass(raw) {
  const num   = parseInt(raw, 10);
  const label = _skillProficiencyLabel(raw).toLowerCase();
  if (label === "beginner"     || num === 0) return "skill-badge-beginner";
  if (label === "elementary"   || num === 1) return "skill-badge-elementary";
  if (label === "intermediate" || num === 2) return "skill-badge-intermediate";
  if (label === "advanced"     || num === 3) return "skill-badge-advanced";
  if (label === "expert"       || num === 4) return "skill-badge-expert";
  return "skill-badge-default";
}

/* ═══ Certifications — Inline Profile Card ═════════════ */

async function _injectProfileCertifications(empNumber) {
  const panel = document.getElementById("empDetailPanel");
  if (!panel || !empNumber) return;

  // Create or reuse placeholder div appended after skills
  let certBlock = document.getElementById("empProfileCertBlock");
  if (!certBlock) {
    certBlock          = document.createElement("div");
    certBlock.id       = "empProfileCertBlock";
    panel.appendChild(certBlock);
  }

  certBlock.style.display = "";
  certBlock.innerHTML = `
    <div class="skill-inline-loading">
      <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
      Loading certifications...
    </div>`;

  try {
    const res    = await fetch(
      `/api/employee/certifications?employeeId=${encodeURIComponent(empNumber)}&t=${Date.now()}`,
      { cache: "no-store" }
    );
    const result = await res.json();

    if (result.status === "success" && result.data && Array.isArray(result.data) && result.data.length > 0) {
      certBlock.innerHTML = _buildProfileCertCard(result.data);
    } else if (result.status === "success") {
      // No certs yet — render empty card with Add button
      certBlock.innerHTML = `
        <div class="detail-card">
          <div class="detail-card-header">
            <i class="bi bi-award-fill"></i> Certifications
            <span class="skill-count-chip" style="margin-left:8px">0 certificates</span>
            <button class="cert-edit-btn" onclick="_openCertModal()" title="Add certificate" style="margin-left:auto">
              <i class="bi bi-plus-lg"></i> Add
            </button>
          </div>
          <div class="cert-inline-body" style="padding:32px 24px;justify-content:center;flex-direction:column;align-items:center;gap:8px;">
            <i class="bi bi-patch-check" style="font-size:2rem;opacity:0.25;color:var(--clr-text-muted);"></i>
            <span style="color:var(--clr-text-muted);font-size:0.85rem;text-align:center;">
              No certifications recorded yet.<br>Click <strong>Add</strong> to add one.
            </span>
          </div>
        </div>`;
    } else {
      certBlock.innerHTML = `
        <div class="detail-card">
          <div class="detail-card-header"><i class="bi bi-award"></i> Certifications</div>
          <div style="padding:16px;color:#dc3545;font-size:0.82rem;">
            <i class="bi bi-exclamation-triangle"></i>
            Could not load certifications: ${_esc(result.message || "Unknown error")}
          </div>
        </div>`;
    }
  } catch (err) {
    certBlock.innerHTML = `
      <div class="detail-card">
        <div class="detail-card-header"><i class="bi bi-award"></i> Certifications</div>
        <div style="padding:16px;color:#dc3545;font-size:0.82rem;">
          <i class="bi bi-wifi-off"></i> Certifications fetch error: ${_esc(err.message)}
        </div>
      </div>`;
  }
}

function _buildProfileCertCard(records) {
  // ── Guard: handle null / undefined / empty gracefully ──
  if (!records || !Array.isArray(records) || records.length === 0) {
    return `
      <div class="detail-card">
        <div class="detail-card-header">
          <i class="bi bi-award-fill"></i> Certifications
          <span class="skill-count-chip" style="margin-left:8px">0 certificates</span>
          <button class="cert-edit-btn" onclick="_openCertModal()" title="Add certificate" style="margin-left:auto">
            <i class="bi bi-plus-lg"></i> Add
          </button>
        </div>
        <div class="cert-inline-body" style="padding:32px 24px;justify-content:center;flex-direction:column;align-items:center;gap:8px;">
          <i class="bi bi-patch-check" style="font-size:2rem;opacity:0.25;color:var(--clr-text-muted);"></i>
          <span style="color:var(--clr-text-muted);font-size:0.85rem;text-align:center;">
            No certifications recorded yet.<br>Click <strong>Add</strong> to add one.
          </span>
        </div>
      </div>`;
  }

  const chips = records.map(c => {
    const name        = _esc(c.Certification_name || c.certification_name || "—");
    const tech        = c.Technology        || c.technology        || "";
    const date        = c.Completion_Date   || c.completion_date   || "";
    const type        = c.Type              || c.type              || "";
    const expiry      = c.Date_of_Expiry    || c.date_of_expiry    || "";
    const amount      = c.Amount_in_Rs      || c.amount_in_rs      || "";

    const dateLabel   = date   && date   !== "null" ? date   : "";
    const expiryLabel = expiry && expiry !== "null" ? expiry : "";
    const amountLabel = amount && amount !== "null" && String(amount) !== "0"
      ? `₹${Number(amount).toLocaleString("en-IN")}` : "";

    const typeBadgeClass = String(type).toLowerCase() === "paid"
      ? "cert-type-paid" : "cert-type-self";

    return `
      <div class="cert-inline-chip">
        <div class="cert-chip-top">
          <span class="cert-inline-name">${name}</span>
          ${type ? `<span class="cert-type-badge ${typeBadgeClass}">${_esc(type)}</span>` : ""}
        </div>
        <div class="cert-chip-meta">
          ${tech        && tech        !== "null" ? `<span class="cert-meta-item"><i class="bi bi-cpu"></i> ${_esc(tech)}</span>`                                     : ""}
          ${dateLabel                             ? `<span class="cert-meta-item"><i class="bi bi-calendar-check"></i> ${_esc(dateLabel)}</span>`                     : ""}
          ${expiryLabel                           ? `<span class="cert-meta-item cert-expiry"><i class="bi bi-hourglass-split"></i> Expires: ${_esc(expiryLabel)}</span>` : ""}
          ${amountLabel                           ? `<span class="cert-meta-item"><i class="bi bi-currency-rupee"></i> ${_esc(amountLabel)}</span>`                   : ""}
        </div>
      </div>`;
  }).join("");

  return `
    <div class="detail-card">
      <div class="detail-card-header">
        <i class="bi bi-award-fill"></i> Certifications
        <span class="skill-count-chip" style="margin-left:8px">${records.length} certificate${records.length !== 1 ? "s" : ""}</span>
        <button class="cert-edit-btn" onclick="_openCertModal()" title="Add certificate" style="margin-left:auto">
          <i class="bi bi-plus-lg"></i> Add
        </button>
      </div>
      <div class="cert-inline-body">${chips}</div>
    </div>`;
}

/* ═══ Certification Modal ════════════════════════════════════════ */

function _openCertModal() {
  _clearCertForm();
  document.getElementById("certModalOverlay").style.display = "flex";
  setTimeout(() => document.getElementById("certFieldName").focus(), 60);
}

function _closeCertModal() {
  document.getElementById("certModalOverlay").style.display = "none";
}

function _clearCertForm() {
  document.getElementById("certFieldName").value       = "";
  document.getElementById("certFieldTech").value       = "";
  document.getElementById("certFieldType").value       = "Self";
  document.getElementById("certFieldAmount").value     = "";
  document.getElementById("certFieldCompletion").value = "";
  document.getElementById("certFieldExpiry").value     = "";
  document.getElementById("certFieldNote").value       = "";
  document.getElementById("certFieldNameError").style.display = "none";
  document.getElementById("certModalError").style.display     = "none";
}

async function _submitCertModal() {
  const name = document.getElementById("certFieldName").value.trim();

  // Validation
  if (!name) {
    document.getElementById("certFieldNameError").style.display = "flex";
    document.getElementById("certFieldName").focus();
    return;
  }
  document.getElementById("certFieldNameError").style.display = "none";

  const emp = _empData[_currentEmpIdx];
  if (!emp) return;

  const empNumber = (emp?.employeeNumber || emp?.employee_code || "").toString().trim();
  if (!empNumber) {
    alert("Employee Number missing!");
    console.error("empNumber missing:", emp);
    return;
  }

  const type   = document.getElementById("certFieldType").value;
  const amount = document.getElementById("certFieldAmount").value.trim();

  const payload = {
    employeeNumber:    empNumber,
    certification_name: name,
    technology:        document.getElementById("certFieldTech").value.trim(),
    type:              type,
    amount_in_rs:      type === "Paid" ? amount : "",
    completion_date:   document.getElementById("certFieldCompletion").value,
    date_of_expiry:    document.getElementById("certFieldExpiry").value,
    note:              document.getElementById("certFieldNote").value.trim(),
  };

  console.log("Payload:", payload);

  const submitBtn = document.getElementById("certSubmitBtn");
  submitBtn.disabled    = true;
  submitBtn.innerHTML   = `<div class="spinner-border spinner-border-sm"></div> Saving...`;

  document.getElementById("certModalError").style.display = "none";

  try {
    const res    = await fetch("/api/employee/certifications", {
      method:  "POST",
      headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
      body:    JSON.stringify(payload),
    });
    const result = await res.json();

    console.log("API Response:", result);

    if (result.status === "success") {
      // ── 1. Close modal immediately ──
      _closeCertModal();

      if (typeof _showToast === "function") {
        _showToast("Certificate added successfully!", "success");
      }

      // ── 2. Show loading state in cert block right away ──
      const certBlock = document.getElementById("empProfileCertBlock");
      if (certBlock) {
        certBlock.innerHTML = `
          <div class="skill-inline-loading">
            <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
            Saving... fetching updated certifications...
          </div>`;
      }

      // ── 3. Wait for Delta Lake to fully commit the write ──
      //    Backend already does REFRESH TABLE + 1s sleep.
      //    We add another 2s on the client side as a buffer.
      await new Promise(r => setTimeout(r, 3000));

      // ── 4. Reload via the single source of truth function ──
      //    This handles all cases: empty, error, populated list.
      await _injectProfileCertifications(empNumber);

    } else {
      document.getElementById("certModalErrorText").textContent =
        result.message || "Failed to save";
      document.getElementById("certModalError").style.display = "flex";
    }

  } catch (err) {
    console.error("Submit cert error:", err);
    document.getElementById("certModalErrorText").textContent =
      "Network error: " + err.message;
    document.getElementById("certModalError").style.display = "flex";
  } finally {
    submitBtn.disabled  = false;
    submitBtn.innerHTML = `<i class="bi bi-plus-circle"></i> Add Certificate`;
  }
}