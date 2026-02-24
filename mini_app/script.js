let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

async function init() {
    try {
        console.log('Initializing app...');
        
        // Аутентификация
        const authResult = await authenticate();
        console.log('Auth result:', authResult);
        
        if (authResult && authResult.ok) {
            userData = authResult.user;
            console.log('User data:', userData);
            
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
            await loadInboxMessages();
            await loadSentMessages();
            
            // Настраиваем интерфейс
            setupTabs();
            setupEventListeners();
            
            // Активируем первую вкладку
            document.querySelector('.tab-btn.active')?.click();
            
        } else {
            console.error('Auth failed:', authResult);
            showError('Ошибка авторизации');
        }
    } catch (error) {
        console.error('Init error:', error);
        showError('Ошибка загрузки');
    }
}

async function authenticate() {
    const initData = tg.initData;
    console.log('Init data:', initData ? 'present' : 'missing');
    
    if (!initData) {
        return { ok: false, error: 'Нет данных авторизации' };
    }
    
    try {
        // Пробуем GET запрос с query параметром
        const response = await fetch(`/api/auth?initData=${encodeURIComponent(initData)}`, {
            method: 'GET',
            headers: { 
                'Accept': 'application/json'
            }
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
            }
        });
    });
}

async function loadInboxMessages() {
    try {
        console.log('Loading inbox messages...');
        const response = await fetch(`/api/messages/inbox?initData=${encodeURIComponent(tg.initData)}`, {
            method: 'GET',
            headers: { 
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            console.error('Inbox response not OK:', response.status);
            displayInboxMessages([]);
            return;
        }
        
        const data = await response.json();
        console.log('Inbox messages loaded:', data.messages?.length || 0);
        displayInboxMessages(data.messages || []);
    } catch (error) {
        console.error('Load inbox error:', error);
        displayInboxMessages([]);
    }
}

async function loadSentMessages() {
    try {
        console.log('Loading sent messages...');
        const response = await fetch(`/api/messages/sent?initData=${encodeURIComponent(tg.initData)}`, {
            method: 'GET',
            headers: { 
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            console.error('Sent response not OK:', response.status);
            displaySentMessages([]);
            return;
        }
        
        const data = await response.json();
        console.log('Sent messages loaded:', data.messages?.length || 0);
        displaySentMessages(data.messages || []);
    } catch (error) {
        console.error('Load sent error:', error);
        displaySentMessages([]);
    }
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
            alert('Ошибка: ' + (result.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        console.error('Send error:', error);
        alert('Ошибка отправки');
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
