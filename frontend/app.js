const chat = document.getElementById("chat");
const form = document.getElementById("ask-form");
const textarea = document.getElementById("question");
const modelSelect = document.getElementById("model");
const micBtn = document.getElementById("mic-btn");
const historyToggle = document.getElementById("toggle-history");
const historyPanel = document.getElementById("history-panel");
const historyList = document.getElementById("history-list");
const clearHistoryBtn = document.getElementById("clear-history");
const closeHistoryBtn = document.getElementById("close-history");

const API_URL = window.location.origin.replace(/:\d+$/, ":8000"); // assume API on 8000

if (modelSelect) {
  modelSelect.value = "gemma3:1b";
}
const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
let recognition = null;
let isListening = false;
const synth = "speechSynthesis" in window ? window.speechSynthesis : null;
let currentUtterance = null;
let voicesLoaded = false;
const HISTORY_KEY = "burkina-qa-history";
let historyEntries = [];

function sanitizeText(text) {
  if (!text) return "";
  return text
    .replace(/\*\*/g, "")
    .replace(/^\s*[-*]\s+/gm, "• ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function appendMessage(role, text, sources = []) {
  const bubble = document.createElement("div");
  bubble.className = `bubble ${role}`;

  if (role === "user") {
    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = "Vous";
    bubble.appendChild(meta);
  } else {
    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = "Assistant";
    bubble.appendChild(meta);
  }

  const content = document.createElement("div");
  content.innerText = sanitizeText(text);
  bubble.appendChild(content);

  if (sources.length) {
    const list = document.createElement("ul");
    list.className = "sources";
    list.innerHTML = sources
      .map((src) => {
        const url = src.payload.url || src.payload.file_name || "Source";
        return `<li><strong>${url}</strong> (score: ${src.score.toFixed(3)})</li>`;
      })
      .join("");
    bubble.appendChild(list);
  }

  if (role === "bot" && synth && sanitizeText(text)) {
    const actions = document.createElement("div");
    actions.className = "actions";
    const speakBtn = document.createElement("button");
    speakBtn.type = "button";
    speakBtn.className = "speak-btn";
    speakBtn.textContent = "🔊 Écouter";
    speakBtn.dataset.state = "idle";
    speakBtn.addEventListener("click", () => {
      speakText(sanitizeText(text), speakBtn);
    });
    actions.appendChild(speakBtn);
    bubble.appendChild(actions);
  }

  chat.appendChild(bubble);
  chat.scrollTop = chat.scrollHeight;
}

function setLoading(isLoading) {
  const submitBtn = form.querySelector("button[type='submit']");
  if (submitBtn) submitBtn.disabled = isLoading;
  textarea.disabled = isLoading;
  if (modelSelect) modelSelect.disabled = isLoading;
  if (micBtn) micBtn.disabled = isLoading || !SpeechRecognition;
}

function showLoadingBubble() {
  const bubble = document.createElement("div");
  bubble.className = "bubble bot loading";
  const spinner = document.createElement("span");
  spinner.className = "spinner";
  const label = document.createElement("span");
  label.textContent = "L'assistant prépare sa réponse...";
  bubble.appendChild(spinner);
  bubble.appendChild(label);
  chat.appendChild(bubble);
  chat.scrollTop = chat.scrollHeight;
  return bubble;
}

function speakText(text, btn) {
  if (!synth) return;
  if (!voicesLoaded) {
    preloadVoices(() => speakText(text, btn));
    return;
  }
  if (currentUtterance && synth.speaking) {
    synth.cancel();
    currentUtterance = null;
    if (btn) {
      btn.textContent = "🔊 Écouter";
      btn.dataset.state = "idle";
    }
    return;
  }
  currentUtterance = new SpeechSynthesisUtterance(text);
  currentUtterance.lang = "fr-FR";
  currentUtterance.rate = 1;
  const voice = synth.getVoices().find((v) => v.lang.startsWith("fr"));
  if (voice) {
    currentUtterance.voice = voice;
  }
  currentUtterance.onstart = () => {
    if (btn) {
      btn.textContent = "⏹️ Arrêter";
      btn.dataset.state = "playing";
    }
  };
  currentUtterance.onend = () => {
    currentUtterance = null;
    if (btn) {
      btn.textContent = "🔊 Écouter";
      btn.dataset.state = "idle";
    }
  };
  currentUtterance.onerror = () => {
    currentUtterance = null;
    if (btn) {
      btn.textContent = "🔊 Écouter";
      btn.dataset.state = "idle";
    }
  };
  synth.cancel();
  synth.speak(currentUtterance);
}

function addHistoryEntry(question, answer, sources) {
  const entry = {
    id: Date.now(),
    question: question.trim(),
    answer,
    sources: (sources || []).map((s) => ({
      source: s.source || s.payload?.url || s.payload?.file_name || "",
      score: s.score || 0,
      payload: s.payload || {},
    })),
    timestamp: new Date().toISOString(),
  };
  historyEntries.unshift(entry);
  if (historyEntries.length > 40) {
    historyEntries = historyEntries.slice(0, 40);
  }
  saveHistory();
  renderHistory();
}

function renderHistory() {
  if (!historyList) return;
  historyList.innerHTML = "";
  if (!historyEntries.length) {
    const empty = document.createElement("li");
    empty.className = "history-empty";
    empty.textContent = "Aucune recherche enregistrée pour le moment.";
    historyList.appendChild(empty);
    return;
  }

  historyEntries.forEach((entry, index) => {
    const li = document.createElement("li");
    li.className = "history-item";

    const question = document.createElement("div");
    question.className = "history-question";
    question.textContent = entry.question;

    const meta = document.createElement("div");
    meta.className = "history-meta";
    meta.textContent = formatTimestamp(entry.timestamp);

    const actions = document.createElement("div");
    actions.className = "history-actions";

    const replayBtn = document.createElement("button");
    replayBtn.type = "button";
    replayBtn.textContent = "Reposer";
    replayBtn.addEventListener("click", () => {
      textarea.value = entry.question;
      textarea.focus();
      if (historyPanel) historyPanel.hidden = true;
    });

    const viewBtn = document.createElement("button");
    viewBtn.type = "button";
    viewBtn.textContent = "Afficher";
    viewBtn.addEventListener("click", () => {
      appendMessage("user", entry.question);
      appendMessage("bot", entry.answer, entry.sources);
      if (historyPanel) historyPanel.hidden = true;
      chat.scrollTop = chat.scrollHeight;
    });

    actions.appendChild(replayBtn);
    actions.appendChild(viewBtn);

    li.appendChild(question);
    li.appendChild(meta);
    li.appendChild(actions);
    historyList.appendChild(li);
  });
}

function formatTimestamp(ts) {
  if (!ts) return "";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString(undefined, {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function saveHistory() {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(historyEntries));
  } catch (err) {
    console.warn("Impossible d'enregistrer l'historique", err);
  }
}

function loadHistory() {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (raw) {
      historyEntries = JSON.parse(raw);
    }
  } catch (err) {
    historyEntries = [];
  }
  renderHistory();
}

async function askQuestion(prompt) {
  const payload = {
    question: prompt,
    top_k: 4,
    score_threshold: 0.4,
    normalize: true,
    timeout: 240,
    ollama_model: modelSelect.value || "gemma3:1b",
  };
  const resp = await fetch(`${API_URL}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const errorText = await resp.text();
    throw new Error(errorText || `Erreur API (${resp.status})`);
  }
  return resp.json();
}

form.addEventListener("submit", async (evt) => {
  evt.preventDefault();
  const question = textarea.value.trim();
  if (!question) return;

  if (recognition && isListening) {
    recognition.stop();
  }
  if (currentUtterance && synth) {
    synth.cancel();
    currentUtterance = null;
  }

  appendMessage("user", question);
  textarea.value = "";
  setLoading(true);
  const loadingBubble = showLoadingBubble();

  try {
    const data = await askQuestion(question);
    if (loadingBubble.parentNode) {
      chat.removeChild(loadingBubble);
    }
    const cleanAnswer = sanitizeText(data.answer);
    appendMessage("bot", data.answer, data.sources);
    addHistoryEntry(question, cleanAnswer, data.sources);
  } catch (err) {
    if (loadingBubble.parentNode) {
      chat.removeChild(loadingBubble);
    }
    appendMessage(
      "bot",
      `Erreur: ${err.message || "Impossible d'obtenir une réponse pour le moment."}`
    );
  } finally {
    setLoading(false);
    textarea.focus();
  }
});

textarea.addEventListener("keydown", (evt) => {
  if (evt.key === "Enter" && !evt.shiftKey) {
    evt.preventDefault();
    form.dispatchEvent(new Event("submit"));
  }
});

function setupSpeechRecognition() {
  if (!SpeechRecognition || !micBtn) {
    if (micBtn) {
      micBtn.disabled = true;
      micBtn.textContent = "🎙️ Non disponible";
      micBtn.title = "La reconnaissance vocale n'est pas supportée par votre navigateur.";
    }
    return;
  }

  recognition = new SpeechRecognition();
  recognition.lang = "fr-FR";
  recognition.interimResults = false;
  recognition.maxAlternatives = 1;

  recognition.onstart = () => {
    isListening = true;
    micBtn.classList.add("active");
    micBtn.textContent = "🎙️ Écoute...";
    micBtn.title = "Cliquez pour arrêter l'enregistrement.";
  };

  recognition.onend = () => {
    isListening = false;
    micBtn.classList.remove("active");
    micBtn.textContent = "🎙️ Parler";
    micBtn.title = "Dicter votre question (français)";
  };

  recognition.onresult = (event) => {
    const transcript = event.results[0][0].transcript.trim();
    if (transcript) {
      textarea.value = textarea.value
        ? `${textarea.value.trim()} ${transcript}`
        : transcript;
      textarea.focus();
    }
  };

  recognition.onerror = (event) => {
    appendMessage(
      "bot",
      event.error === "not-allowed"
        ? "Erreur dictée : accès au micro refusé."
        : "Erreur dictée : impossible d'utiliser la reconnaissance vocale."
    );
  };

  micBtn.addEventListener("click", () => {
    if (!recognition) return;
    if (isListening) {
      recognition.stop();
    } else {
      try {
        recognition.start();
      } catch (error) {
        appendMessage("bot", "Erreur : démarrage de la dictée impossible.");
      }
    }
  });
}

setupSpeechRecognition();
preloadVoices();
loadHistory();

if (historyToggle && historyPanel) {
  historyToggle.addEventListener("click", () => {
    const willShow = historyPanel.hidden;
    if (willShow) {
      renderHistory();
    }
    historyPanel.hidden = !historyPanel.hidden;
  });
}

if (closeHistoryBtn && historyPanel) {
  closeHistoryBtn.addEventListener("click", () => {
    historyPanel.hidden = true;
  });
}

if (clearHistoryBtn) {
  clearHistoryBtn.addEventListener("click", () => {
    if (!historyEntries.length) return;
    const confirmClear = confirm("Effacer tout l'historique ?");
    if (confirmClear) {
      historyEntries = [];
      saveHistory();
      renderHistory();
    }
  });
}

function preloadVoices(callback) {
  if (!synth) {
    voicesLoaded = false;
    return;
  }
  const voices = synth.getVoices();
  if (voices.length) {
    voicesLoaded = true;
    if (callback) callback();
    return;
  }
  const handler = () => {
    voicesLoaded = true;
    if (typeof synth.removeEventListener === "function") {
      synth.removeEventListener("voiceschanged", handler);
    } else {
      synth.onvoiceschanged = null;
    }
    if (callback) callback();
  };
  if (typeof synth.addEventListener === "function") {
    synth.addEventListener("voiceschanged", handler);
  } else {
    synth.onvoiceschanged = handler;
  }
  synth.getVoices();
}
