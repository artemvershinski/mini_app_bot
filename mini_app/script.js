let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

// Функция для подробного логирования
function logWithDetails(level, message, data = null) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${level}] ${message}`;
    
    if (data) {
        console.log(logMessage, data);
        // Пытаемся отправить логи на сервер
        fetch('/api/log', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                level,
                message,
                data,
                url: window.location.href,
                userAgent: navigator.userAgent
            })
        }).catch(() => {}); // Игнорируем ошибки отправки логов
    } else {
        console.log(logMessage);
    }
}

async function init() {
    try {
        logWithDetails('INFO', 'Initializing app...');
        
        // Аутентификация
        const authResult = await authenticate();
        logWithDetails('INFO', 'Auth result:', authResult);
        
        if (authResult && authResult.ok) {
            userData = authResult.user;
            logWithDetails('INFO', 'User data loaded:', userData);
            
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
            logWithDetails('INFO', 'Loading messages...');
            await loadInboxMessages();
            await loadSentMessages();
            
            // Настраиваем интерфейс
            setupTabs();
            setupEventListeners();
            
            // Активируем первую вкладку
            document.querySelector('.tab-btn.active')?.click();
            
        } else {
            logWithDetails('ERROR', 'Auth failed:', authResult);
            showError('Ошибка авторизации: ' + (authResult?.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        logWithDetails('ERROR', 'Init error:', error);
        showError('Ошибка загрузки: ' + error.message);
    }
}

async function authenticate() {
    const initData = tg.initData;
    logWithDetails('INFO', 'Init data present:', { 
        hasInitData: !!initData,
        length: initData?.length 
    });
    
    if (!initData) {
        logWithDetails('ERROR', 'No init data');
        return { ok: false, error: 'Нет данных авторизации' };
    }
    
    // Пробуем разные методы авторизации
    const methods = [
        // Метод 1: GET с query параметром
        async () => {
            logWithDetails('INFO', 'Trying auth method 1: GET with query param');
            const url = `/api/auth?initData=${encodeURIComponent(initData)}`;
            logWithDetails('DEBUG', 'Request URL:', url);
            
            const response = await fetch(url, {
                method: 'GET',
                headers: { 
                    'Accept': 'application/json'
                }
            });
            
            logWithDetails('DEBUG', 'Response status:', response.status);
            const data = await response.json();
            logWithDetails('DEBUG', 'Response data:', data);
            return { response, data };
        },
        
        // Метод 2: POST с JSON
        async () => {
            logWithDetails('INFO', 'Trying auth method 2: POST with JSON');
            const response = await fetch('/api/auth', {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({ initData })
            });
            
            logWithDetails('DEBUG', 'Response status:', response.status);
            const data = await response.json();
            logWithDetails('DEBUG', 'Response data:', data);
            return { response, data };
        },
        
        // Метод 3: POST с form data
        async () => {
            logWithDetails('INFO', 'Trying auth method 3: POST with form data');
            const formData = new FormData();
            formData.append('initData', initData);
            
            const response = await fetch('/api/auth', {
                method: 'POST',
                body: formData
            });
            
            logWithDetails('DEBUG', 'Response status:', response.status);
            const text = await response.text();
            logWithDetails('DEBUG', 'Response text:', text);
            try {
                const data = JSON.parse(text);
                return { response, data };
            } catch {
                return { response, data: { ok: false, error: text } };
            }
        }
    ];
    
    // Пробуем методы по очереди
    for (let i = 0; i < methods.length; i++) {
        try {
            const result = await methods[i]();
            
            if (result.response.ok) {
                logWithDetails('INFO', `Auth method ${i+1} succeeded:`, result.data);
                return result.data;
            } else {
                logWithDetails('WARN', `Auth method ${i+1} failed with status ${result.response.status}:`, result.data);
            }
        } catch (error) {
            logWithDetails('ERROR', `Auth method ${i+1} threw error:`, error);
        }
    }
    
    logWithDetails('ERROR', 'All auth methods failed');
    return { ok: false, error: 'Все методы авторизации не сработали' };
}

async function loadInboxMessages() {
    try {
        logWithDetails('INFO', 'Loading inbox messages...');
        const initData = tg.initData;
        
        const response = await fetch(`/api/messages/inbox?initData=${encodeURIComponent(initData)}`, {
            method: 'GET',
            headers: { 
                'Accept': 'application/json'
            }
        });
        
        logWithDetails('DEBUG', 'Inbox response status:', response.status);
        
        if (!response.ok) {
            logWithDetails('ERROR', 'Inbox response not OK:', response.status);
            displayInboxMessages([]);
            return;
        }
        
        const data = await response.json();
        logWithDetails('INFO', 'Inbox messages loaded:', { count: data.messages?.length || 0 });
        displayInboxMessages(data.messages || []);
    } catch (error) {
        logWithDetails('ERROR', 'Load inbox error:', error);
        displayInboxMessages([]);
    }
}

async function loadSentMessages() {
    try {
        logWithDetails('INFO', 'Loading sent messages...');
        const initData = tg.initData;
        
        const response = await fetch(`/api/messages/sent?initData=${encodeURIComponent(initData)}`, {
            method: 'GET',
            headers: { 
                'Accept': 'application/json'
            }
        });
        
        logWithDetails('DEBUG', 'Sent response status:', response.status);
        
        if (!response.ok) {
            logWithDetails('ERROR', 'Sent response not OK:', response.status);
            displaySentMessages([]);
            return;
        }
        
        const data = await response.json();
        logWithDetails('INFO', 'Sent messages loaded:', { count: data.messages?.length || 0 });
        displaySentMessages(data.messages || []);
    } catch (error) {
        logWithDetails('ERROR', 'Load sent error:', error);
        displaySentMessages([]);
    }
}

function setupTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabs = document.querySelectorAll('.tab');
    
    logWithDetails('INFO', 'Setting up tabs:', { buttons: tabButtons.length, tabs: tabs.length });
    
    tabButtons.forEach(btn => {
        btn.addEventListener('click', function(e) {
            const tabId = this.dataset.tab;
            logWithDetails('DEBUG', 'Tab clicked:', tabId);
            
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
                logWithDetails('DEBUG', 'Activated tab:', tabId + '-tab');
            }
        });
    });
}

function displayInboxMessages(messages) {
    const container = document.getElementById('inboxMessages');
    if (!container) return;
    
    if (!messages || messages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📨</div>
                <h3>Нет ответов</h3>
                <p>Когда админ ответит, они появятся здесь</p>
            </div>
        `;
        return;
    }
    
    let html = '';
    messages.forEach(msg => {
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
                    <span class="message-id">Ответ на #${msg.message_id}</span>
                    <span class="message-time">${timeStr}</span>
                </div>
                
                <div class="answer-badge" style="margin-top: 0;">
                    <div class="answer-header">Администратор:</div>
                    <div class="answer-text">
                        ${escapeHtml(msg.answer_text || 'Ответ получен')}
                    </div>
                    <div class="answer-meta">
                        ${msg.answered_by_name || 'Администратор'}
                    </div>
                </div>
                
                <div style="margin-top: 12px; padding-top: 12px; border-top: var(--border-light);">
                    <div style="font-size: 13px; color: var(--text-tertiary); margin-bottom: 4px;">
                        Ваше сообщение:
                    </div>
                    <div style="font-size: 14px; color: var(--text-secondary);">
                        ${escapeHtml(msg.original_text || '')}
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

function displaySentMessages(messages) {
    const container = document.getElementById('sentMessages');
    if (!container) return;
    
    if (!messages || messages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📭</div>
                <h3>Нет сообщений</h3>
                <p>Напишите первое сообщение</p>
            </div>
        `;
        return;
    }
    
    let html = '';
    messages.forEach(msg => {
        const date = msg.forwarded_at ? new Date(msg.forwarded_at) : new Date();
        const timeStr = date.toLocaleString('ru-RU', {
            hour: '2-digit',
            minute: '2-digit',
            day: '2-digit',
            month: '2-digit'
        });
        
        const statusClass = msg.is_answered ? 'status-answered' : 'status-waiting';
        const statusText = msg.is_answered ? 'Отвечено' : 'Ожидает ответа';
        
        html += `
            <div class="message-card">
                <div class="message-header">
                    <span class="message-id">#${msg.message_id}</span>
                    <span class="message-time">${timeStr}</span>
                </div>
                
                <div class="message-status ${statusClass}">
                    ${statusText}
                </div>
                
                <div class="message-text">
                    ${escapeHtml(msg.text || '')}
                </div>
                
                ${msg.is_answered ? `
                    <div class="answer-badge">
                        <div class="answer-header">Ответ:</div>
                        <div class="answer-text">
                            ${escapeHtml(msg.answer_text || 'Ответ получен')}
                        </div>
                    </div>
                ` : ''}
            </div>
        `;
    });
    
    container.innerHTML = html;
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
        logWithDetails('INFO', 'Sending message...', { textLength: text.length });
        
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
        
        logWithDetails('DEBUG', 'Send response status:', response.status);
        
        const result = await response.json();
        logWithDetails('INFO', 'Send result:', result);
        
        if (result.ok) {
            textarea.value = '';
            updateCharCounter();
            updateButtonState();
            
            // Обновляем сообщения
            await loadSentMessages();
            
            // Показываем успех
            if (tg.showPopup) {
                tg.showPopup({
                    title: 'Успешно',
                    message: `Сообщение #${result.message_id} отправлено!`,
                    buttons: [{ type: 'ok' }]
                });
            }
        } else {
            logWithDetails('ERROR', 'Send failed:', result);
            alert('Ошибка: ' + (result.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        logWithDetails('ERROR', 'Send error:', error);
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
    logWithDetails('ERROR', 'Showing error:', message);
    
    const container = document.getElementById('inboxMessages');
    if (container) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">⚠️</div>
                <h3>Ошибка</h3>
                <p>${message}</p>
                <button onclick="location.reload()" style="margin-top: 16px; padding: 12px 24px; background: var(--accent-gradient); border: none; border-radius: var(--radius-base); color: var(--text-inverse); font-weight: 600; cursor: pointer;">
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
