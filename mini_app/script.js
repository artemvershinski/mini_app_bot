let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

// Проверяем версию Telegram Web App
const tgVersion = tg.version || '6.0';
console.log('Telegram Web App version:', tgVersion);

// Безопасный показ попапа
function safeShowPopup(params) {
    // Проверяем, поддерживается ли showPopup
    if (tg.version && parseFloat(tg.version) >= 6.2) {
        try {
            tg.showPopup(params);
        } catch (e) {
            console.log('Popup error:', e);
            // Fallback - alert если совсем ничего
            if (params.message) {
                alert(params.message);
            }
        }
    } else {
        // Для старых версий просто показываем alert
        console.log('Popup message:', params.message);
        if (params.message) {
            alert(params.message);
        }
    }
}

async function init() {
    try {
        console.log('Initializing app...');
        
        // Аутентификация
        const authResult = await authenticate();
        console.log('Auth result:', authResult);
        
        if (authResult && authResult.ok) {
            userData = authResult.user;
            
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
            
            // Загружаем сообщения для обеих вкладок
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
            
            // Настраиваем переключение табов
            setupTabs();
            setupEventListeners();
        } else {
            console.error('Auth failed:', authResult);
            // Показываем ошибку но не ломаем приложение
            const errorMsg = authResult?.error || 'Ошибка авторизации';
            const container = document.getElementById('inboxMessages');
            if (container) {
                container.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-icon">⚠️</div>
                        <h3>Ошибка подключения</h3>
                        <p>${errorMsg}</p>
                        <button onclick="location.reload()" style="margin-top: 16px; padding: 12px 24px; background: var(--accent-gradient); border: none; border-radius: var(--radius-base); color: var(--text-inverse); font-weight: 600; cursor: pointer;">
                            Обновить
                        </button>
                    </div>
                `;
            }
        }
    } catch (error) {
        console.error('Init error:', error);
        // Показываем ошибку без попапа
        const container = document.getElementById('inboxMessages');
        if (container) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">⚠️</div>
                    <h3>Ошибка загрузки</h3>
                    <p>${error.message || 'Неизвестная ошибка'}</p>
                    <button onclick="location.reload()" style="margin-top: 16px; padding: 12px 24px; background: var(--accent-gradient); border: none; border-radius: var(--radius-base); color: var(--text-inverse); font-weight: 600; cursor: pointer;">
                        Обновить
                    </button>
                </div>
            `;
        }
    }
}

async function authenticate() {
    const initData = tg.initData;
    console.log('Init data length:', initData?.length);
    
    if (!initData) {
        return { ok: false, error: 'Нет данных авторизации' };
    }
    
    try {
        const response = await fetch('/api/auth', {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            body: JSON.stringify({ initData })
        });
        
        if (!response.ok) {
            const text = await response.text();
            console.error('Auth response not OK:', response.status, text);
            return { ok: false, error: `HTTP ${response.status}` };
        }
        
        const data = await response.json();
        console.log('Auth response data:', data);
        return data;
    } catch (error) {
        console.error('Auth fetch error:', error);
        return { ok: false, error: error.message };
    }
}

function setupTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabs = document.querySelectorAll('.tab');
    
    console.log('Setting up tabs:', tabButtons.length, 'buttons,', tabs.length, 'tabs');
    
    tabButtons.forEach(btn => {
        btn.addEventListener('click', function(e) {
            const tabId = this.dataset.tab;
            console.log('Tab clicked:', tabId);
            
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
                console.log('Activated tab:', tabId + '-tab');
            } else {
                console.error('Tab not found:', tabId + '-tab');
            }
        });
    });
}

async function loadInboxMessages() {
    try {
        console.log('Loading inbox messages...');
        const response = await fetch('/api/messages/inbox', {
            headers: { 
                'X-Telegram-Init-Data': tg.initData,
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            console.error('Inbox response not OK:', response.status);
            return;
        }
        
        const data = await response.json();
        console.log('Inbox messages loaded:', data.messages?.length || 0);
        
        if (data.messages) {
            displayInboxMessages(data.messages);
        }
    } catch (error) {
        console.error('Load inbox error:', error);
    }
}

async function loadSentMessages() {
    try {
        console.log('Loading sent messages...');
        const response = await fetch('/api/messages/sent', {
            headers: { 
                'X-Telegram-Init-Data': tg.initData,
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            console.error('Sent response not OK:', response.status);
            return;
        }
        
        const data = await response.json();
        console.log('Sent messages loaded:', data.messages?.length || 0);
        
        if (data.messages) {
            displaySentMessages(data.messages);
        }
    } catch (error) {
        console.error('Load sent error:', error);
    }
}

function displayInboxMessages(messages) {
    const container = document.getElementById('inboxMessages');
    if (!container) {
        console.error('Inbox container not found');
        return;
    }
    
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
    if (!container) {
        console.error('Sent container not found');
        return;
    }
    
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

async function sendMessage(text) {
    if (isLoading) return;
    
    isLoading = true;
    updateButtonState();
    
    try {
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
        
        const result = await response.json();
        
        if (result.ok) {
            safeShowPopup({
                title: 'Успешно',
                message: `Сообщение #${result.message_id} отправлено!`,
                buttons: [{ type: 'ok' }]
            });
            
            const textarea = document.getElementById('messageText');
            if (textarea) {
                textarea.value = '';
                updateCharCounter();
            }
            
            // Обновляем сообщения
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
        } else {
            safeShowPopup({
                title: 'Ошибка',
                message: result.error || 'Ошибка отправки',
                buttons: [{ type: 'cancel' }]
            });
        }
    } catch (error) {
        console.error('Send error:', error);
        safeShowPopup({
            title: 'Ошибка',
            message: 'Ошибка отправки',
            buttons: [{ type: 'cancel' }]
        });
    } finally {
        isLoading = false;
        updateButtonState();
    }
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

function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
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
                const text = textarea.value.trim();
                if (text && !isLoading) {
                    sendMessage(text);
                }
            }
        });
    }
    
    if (sendBtn) {
        sendBtn.addEventListener('click', () => {
            const textarea = document.getElementById('messageText');
            const text = textarea?.value.trim();
            if (text && !isLoading) {
                sendMessage(text);
            }
        });
    }
}

// Запускаем при загрузке
document.addEventListener('DOMContentLoaded', init);
