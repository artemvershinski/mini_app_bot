let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

async function init() {
    try {
        console.log('Initializing...');
        
        // Аутентификация
        const authResult = await authenticate();
        console.log('Auth result:', authResult);
        
        if (authResult && authResult.ok) {
            userData = authResult.user;
            
            document.getElementById('userName').textContent = 
                userData.first_name || userData.username || 'User';
            
            if (userData.unanswered > 0) {
                document.getElementById('unansweredBadge').textContent = userData.unanswered;
                document.getElementById('unansweredBadge').classList.remove('hidden');
            }
            
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
            
            setupTabs();
            setupEventListeners();
        } else {
            showError('Ошибка авторизации: ' + (authResult?.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        console.error('Init error:', error);
        showError('Ошибка загрузки: ' + error.message);
    }
}

async function authenticate() {
    const initData = tg.initData;
    
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
            return { ok: false, error: `HTTP ${response.status}` };
        }
        
        return await response.json();
    } catch (error) {
        return { ok: false, error: error.message };
    }
}

async function loadInboxMessages() {
    try {
        const response = await fetch('/api/messages/inbox', {
            headers: { 'X-Telegram-Init-Data': tg.initData }
        });
        
        if (!response.ok) {
            console.error('Inbox response not OK:', response.status);
            displayInboxMessages([]);
            return;
        }
        
        const data = await response.json();
        displayInboxMessages(data.messages || []);
    } catch (error) {
        console.error('Load inbox error:', error);
        displayInboxMessages([]);
    }
}

async function loadSentMessages() {
    try {
        const response = await fetch('/api/messages/sent', {
            headers: { 'X-Telegram-Init-Data': tg.initData }
        });
        
        if (!response.ok) {
            console.error('Sent response not OK:', response.status);
            displaySentMessages([]);
            return;
        }
        
        const data = await response.json();
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

function setupTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabs = document.querySelectorAll('.tab');
    
    tabButtons.forEach(btn => {
        btn.addEventListener('click', function() {
            const tabId = this.dataset.tab;
            
            tabButtons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            
            tabs.forEach(t => t.classList.remove('active'));
            document.getElementById(tabId + '-tab').classList.add('active');
        });
    });
}

function setupEventListeners() {
    const textarea = document.getElementById('messageText');
    const sendBtn = document.getElementById('sendMessageBtn');
    
    if (textarea) {
        textarea.addEventListener('input', () => {
            const length = textarea.value.length;
            document.getElementById('charCounter').textContent = length + '/4096';
            
            const hasText = textarea.value.trim().length > 0;
            sendBtn.disabled = !hasText || isLoading;
            if (hasText && !isLoading) {
                sendBtn.classList.add('active');
            } else {
                sendBtn.classList.remove('active');
            }
        });
    }
    
    if (sendBtn) {
        sendBtn.addEventListener('click', sendMessage);
    }
}

async function sendMessage() {
    if (isLoading) return;
    
    const textarea = document.getElementById('messageText');
    const text = textarea.value.trim();
    if (!text) return;
    
    isLoading = true;
    const sendBtn = document.getElementById('sendMessageBtn');
    sendBtn.disabled = true;
    sendBtn.classList.remove('active');
    
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
            document.getElementById('charCounter').textContent = '0/4096';
            
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
            
            if (tg.showPopup) {
                tg.showPopup({
                    title: 'Успешно',
                    message: `Сообщение #${result.message_id} отправлено!`,
                    buttons: [{ type: 'ok' }]
                });
            } else {
                alert(`Сообщение #${result.message_id} отправлено!`);
            }
        } else {
            alert('Ошибка: ' + (result.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        console.error('Send error:', error);
        alert('Ошибка отправки: ' + error.message);
    } finally {
        isLoading = false;
        const hasText = textarea.value.trim().length > 0;
        sendBtn.disabled = !hasText;
        if (hasText) sendBtn.classList.add('active');
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

document.addEventListener('DOMContentLoaded', init);
