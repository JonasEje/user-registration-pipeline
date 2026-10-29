const registerButton = document.querySelector("#register-button");
const refreshButton = document.querySelector("#refresh-button");
const statusLine = document.querySelector("#status");
const statsBody = document.querySelector("#stats-body");
const emptyState = document.querySelector("#empty-state");

const CONSUMER_REFRESH_DELAY_MS = 1200;

function setStatus(message, kind) {
  statusLine.textContent = message;
  statusLine.classList.remove("error", "success");
  if (kind) {
    statusLine.classList.add(kind);
  }
}

function renderStats(days) {
  statsBody.replaceChildren();
  for (const day of days) {
    const row = document.createElement("tr");
    const dateCell = document.createElement("td");
    dateCell.textContent = day.registration_date;
    const countCell = document.createElement("td");
    countCell.textContent = String(day.new_users);
    row.append(dateCell, countCell);
    statsBody.append(row);
  }
  emptyState.hidden = days.length > 0;
}

async function loadStats() {
  const response = await fetch("/api/registrations/stats");
  if (!response.ok) {
    throw new Error("Could not load the daily counts.");
  }
  const payload = await response.json();
  renderStats(payload.days);
}

async function register() {
  registerButton.disabled = true;
  refreshButton.disabled = true;
  setStatus("Sending your registration…");

  try {
    const response = await fetch("/api/registrations", { method: "POST" });
    if (!response.ok) {
      throw new Error("The registration could not be sent.");
    }
    const registration = await response.json();
    setStatus(
      `Registered ${registration._id.slice(0, 8)}… refreshing counts.`,
      "success",
    );
    await new Promise((resolve) =>
      setTimeout(resolve, CONSUMER_REFRESH_DELAY_MS),
    );
    await loadStats();
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    registerButton.disabled = false;
    refreshButton.disabled = false;
  }
}

async function refresh() {
  refreshButton.disabled = true;
  try {
    await loadStats();
    setStatus("Counts refreshed.", "success");
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    refreshButton.disabled = false;
  }
}

registerButton.addEventListener("click", register);
refreshButton.addEventListener("click", refresh);

loadStats().catch((error) => {
  setStatus(error.message, "error");
});
