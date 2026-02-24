let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

// Простое логирование
function log(level, message, data = null) {
    const timestamp = new Date().toISOString();
    if (data) {
        console.log(`[${timestamp}] [${level}] ${message}`, data);
    } else {
        console.log(`[${timestamp}] [${level}] ${message}`);
    }
}

async function init() {
    try {
        log('INFO', 'Initializing app...');
        
        // Аутентификация
        const authResult = await authenticate();
        log('INFO', 'Auth result:', authResult);
        
        if (authResult && authResult.ok) {
            userData = authResult.user;
            log('INFO', 'User data loaded:', userData);
            
            // Обновляем UI
            const userNameEl = document.getElementById('userName');
            if (userNameEl) {
                userNameEl.textContent = userData.first_name || userData.username || 'User';
            }
            
            if (userData.unanswered > 0) {
                const badge = document.getElementById('unansweredBadge');
                if (badge) {
                    badge.textContent = userData.unanswered;
                    badge.classList.remove('hidden');
                }
            }
            
            // Загружаем сообщения
            await loadMessages();
            
            // Настраиваем интерфейс
            setupTabs();
            setupEventListeners();
            
            // Активируем первую вкладку
            document.querySelector('.tab-btn.active')?.click();
            
        } else {
            log('ERROR', 'Auth failed:', authResult);
            showError('Ошибка авторизации: ' + (authResult?.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        log('ERROR', 'Init error:', error);
        showError('Ошибка загрузки: ' + error.message);
    }
}

async function authenticate() {
    const initData = tg.initData;
    log('INFO', 'Init data present:', { hasInitData: !!initData, length: initData?.length });
    
    if (!initData) {
        return { ok: false, error: 'Нет данных авторизации' };
    }
    
    try {
        // Пробуем GET запрос
        const url = `/api/auth?initData=${encodeURIComponent(initData)}`;
        log('DEBUG', 'Auth URL:', url);
        
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        log('DEBUG', 'Auth response status:', response.status);
        
        if (!response.ok) {
            const text = await response.text();
            log('ERROR', 'Auth failed:', { status: response.status, text });
            return { ok: false, error: `HTTP ${response.status}` };
        }
        
        const data = await response.json();
        log('DEBUG', 'Auth response data:', data);
        return data;
    } catch (error) {
        log('ERROR', 'Auth fetch error:', error);
        return { ok: false, error: error.message };
    }
}

async function loadMessages() {
    try {
        log('INFO', 'Loading messages...');
        const initData = tg.initData;
        
        const response = await fetch(`/api/messages?initData=${encodeURIComponent(initData)}`, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        log('DEBUG', 'Messages response status:', response.status);
        
        if (!response.ok) {
            log('ERROR', 'Messages response not OK:', response.status);
            displayMessages([]);
            return;
        }
        
        const data = await response.json();
        log('INFO', 'Messages loaded:', { count: data.messages?.length || 0 });
        displayMessages(data.messages || []);
    } catch (error) {
        log('ERROR', 'Load messages error:', error);
        displayMessages([]);
    }
}

function displayMessages(messages) {
    // Разделяем на отправленные и отвеченные
    const sentContainer = document.getElementById('sentMessages');
    const inboxContainer = document.getElementById('inboxMessages');
    
    if (!sentContainer || !inboxContainer) return;
    
    const sentMessages = messages.filter(msg => !msg.is_answered);
    const answeredMessages = messages.filter(msg => msg.is_answered);
    
    // Отображаем отправленные
    if (sentMessages.length === 0) {
        sentContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📭</div>
                <h3>Нет сообщений</h3>
                <p>Напишите первое сообщение</p>
            </div>
        `;
    } else {
        let html = '';
        sentMessages.forEach(msg => {
            const date = msg.created_at ? new Date(msg.created_at) : new Date();
            const timeStr = date.toLocaleString('ru-RU', {
                hour: '2-digit',
                minute: '2-digit',
                day: '2-digit',
                month: '2-digit'
            });
            
            html += `
                <div class="message-card">
                    <div class="message-header">
                        <span class="message-id">#${msg.id}</span>
                        <span class="message-time">${timeStr}</span>
                    </div>
                    <div class="message-status status-waiting">Ожидает ответа</div>
                    <div class="message-text">${escapeHtml(msg.message_text || '')}</div>
                </div>
            `;
        });
        sentContainer.innerHTML = html;
    }
    
    // Отображаем отвеченные
    if (answeredMessages.length === 0) {
        inboxContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📨</div>
                <h3>Нет ответов</h3>
                <p>Когда админ ответит, они появятся здесь</p>
            </div>
        `;
    } else {
        let html = '';
        answeredMessages.forEach(msg => {
            const date = msg.answered_at ? new Date(msg.answered_at) : new Date();
            const timeStr = date.toLocaleString('ru-RU', {
                hour: '2-digit',
                minute: '2-digit',
                day: '2-digit',
                month: '2-digit'
            });
            
            html += `
                <div class="message-card">
                    <div class="message-header">
                        <span class="message-id">Ответ на #${msg.id}</span>
                        <span class="message-time">${timeStr}</span>
                    </div>
                    <div class="answer-badge" style="margin-top: 0;">
                        <div class="answer-header">Администратор:</div>
                        <div class="answer-text">${escapeHtml(msg.answer_text || 'Ответ получен')}</div>
                    </div>
                    <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1);">
                        <div style="font-size: 13px; color: rgba(255,255,255,0.5); margin-bottom: 4px;">Ваше сообщение:</div>
                        <div style="font-size: 14px; color: rgba(255,255,255,0.7);">${escapeHtml(msg.message_text || '')}</div>
                    </div>
                </div>
            `;
        });
        inboxContainer.innerHTML = html;
    }
}

function setupTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabs = document.querySelectorAll('.tab');
    
    log('INFO', 'Setting up tabs:', { buttons: tabButtons.length, tabs: tabs.length });
    
    tabButtons.forEach(btn => {
        btn.addEventListener('click', function(e) {
            const tabId = this.dataset.tab;
            log('DEBUG', 'Tab clicked:', tabId);
            
            // Убираем активный класс у всех кнопок
            tabButtons.forEach(b => b.classList.remove('active'));
            // Добавляем активный класс нажатой кнопке
            this.classList.add('active');
            
            // Прячем все табы
            tabs.forEach(t => t.classList.remove('active'));
            // Показываем нужный таб
            const activeTab = document.getElementById(tabId + '-tab');
            if (activeTab) {
                activeTab.classList.add('active');
                log('DEBUG', 'Activated tab:', tabId + '-tab');
            }
        });
    });
}

function updateCharCounter() {
    const textarea = document.getElementById('messageText');
    const counter = document.getElementById('charCounter');
    if (textarea && counter) {
        const length = textarea.value.length;
        counter.textContent = `${length}/4096`;
    }
}

function updateButtonState() {
    const textarea = document.getElementById('messageText');
    const button = document.getElementById('sendMessageBtn');
    
    if (textarea && button) {
        const hasText = textarea.value.trim().length > 0;
        
        if (hasText && !isLoading) {
            button.classList.add('active');
            button.disabled = false;
        } else {
            button.classList.remove('active');
            button.disabled = true;
        }
    }
}

async function sendMessage() {
    if (isLoading) return;
    
    const textarea = document.getElementById('messageText');
    if (!textarea) return;
    
    const text = textarea.value.trim();
    if (!text) return;
    
    isLoading = true;
    updateButtonState();
    
    try {
        log('INFO', 'Sending message...', { textLength: text.length });
        
        const response = await fetch('/api/send', {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            body: JSON.stringify({
                initData: tg.initData,
                text: text
            })
        });
        
        log('DEBUG', 'Send response status:', response.status);
        
        const result = await response.json();
        log('INFO', 'Send result:', result);
        
        if (result.ok) {
            textarea.value = '';
            updateCharCounter();
            updateButtonState();
            
            // Обновляем сообщения
            await loadMessages();
            
            // Показываем успех
            if (tg.showPopup) {
                tg.showPopup({
                    title: 'Успешно',
                    message: `Сообщение #${result.message_id} отправлено!`,
                    buttons: [{ type: 'ok' }]
                });
            }
        } else {
            log('ERROR', 'Send failed:', result);
            alert('Ошибка: ' + (result.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        log('ERROR', 'Send error:', error);
        alert('Ошибка отправки: ' + error.message);
    } finally {
        isLoading = false;
        updateButtonState();
    }
}

function setupEventListeners() {
    const textarea = document.getElementById('messageText');
    const sendBtn = document.getElementById('sendMessageBtn');
    
    if (textarea) {
        textarea.addEventListener('input', () => {
            updateCharCounter();
            updateButtonState();
        });
        
        textarea.addEventListener('keydown', (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
                e.preventDefault();
                sendMessage();
            }
        });
    }
    
    if (sendBtn) {
        sendBtn.addEventListener('click', sendMessage);
    }
}

function showError(message) {
    log('ERROR', 'Showing error:', message);
    
    const container = document.getElementById('inboxMessages');
    if (container) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">⚠️</div>
                <h3>Ошибка</h3>
                <p>${message}</p>
                <button onclick="location.reload()" style="margin-top: 16px; padding: 12px 24px; background: linear-gradient(135deg, #9B59B6 0%, #6C3483 100%); border: none; border-radius: 12px; color: white; font-weight: 600; cursor: pointer;">
                    Обновить
                </button>
            </div>
        `;
    }
}

function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// Запускаем при загрузке
document.addEventListener('DOMContentLoaded', init);
