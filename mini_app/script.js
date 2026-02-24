let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

async function init() {
    try {
        showLoading(true);
        
        const authResult = await authenticate();
        
        if (authResult.ok) {
            userData = authResult.user;
            
            document.getElementById('userName').textContent = 
                userData.first_name || userData.username || 'User';
            
            if (userData.unanswered > 0) {
                const badge = document.getElementById('unansweredBadge');
                badge.textContent = userData.unanswered;
                badge.classList.remove('hidden');
            }
            
            // Загружаем сообщения для обеих вкладок
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
            
            setupEventListeners();
        } else {
            showError('Ошибка авторизации');
        }
    } catch (error) {
        console.error('Init error:', error);
        showError('Не удалось загрузить данные');
    } finally {
        showLoading(false);
    }
}

async function authenticate() {
    const initData = tg.initData;
    
    if (!initData) {
        return { ok: false, error: 'No init data' };
    }
    
    try {
        const response = await fetch('/api/auth', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ initData })
        });
        
        return await response.json();
    } catch (error) {
        console.error('Auth error:', error);
        return { ok: false, error: error.message };
    }
}

async function loadInboxMessages() {
    try {
        const response = await fetch('/api/messages/inbox', {
            headers: { 'X-Telegram-Init-Data': tg.initData }
        });
        
        const data = await response.json();
        
        if (data.messages) {
            displayInboxMessages(data.messages);
        }
    } catch (error) {
        console.error('Load inbox error:', error);
    }
}

async function loadSentMessages() {
    try {
        const response = await fetch('/api/messages/sent', {
            headers: { 'X-Telegram-Init-Data': tg.initData }
        });
        
        const data = await response.json();
        
        if (data.messages) {
            displaySentMessages(data.messages);
        }
    } catch (error) {
        console.error('Load sent error:', error);
    }
}

function displayInboxMessages(messages) {
    const container = document.getElementById('inboxMessages');
    
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
        const date = new Date(msg.answered_at || msg.forwarded_at);
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
                        ${msg.answered_by_name ? `От: ${msg.answered_by_name}` : 'Администратор'}
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
        const date = new Date(msg.forwarded_at);
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
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                initData: tg.initData,
                text: text
            })
        });
        
        const result = await response.json();
        
        if (result.ok) {
            tg.showPopup({
                title: 'Успешно',
                message: `Сообщение #${result.message_id} отправлено!`,
                buttons: [{ type: 'ok' }]
            });
            
            document.getElementById('messageText').value = '';
            updateCharCounter();
            updateButtonState();
            
            // Обновляем обе вкладки
            await Promise.all([
                loadInboxMessages(),
                loadSentMessages()
            ]);
        } else {
            showError(result.error || 'Ошибка отправки');
        }
    } catch (error) {
        console.error('Send error:', error);
        showError('Ошибка отправки');
    } finally {
        isLoading = false;
        updateButtonState();
    }
}

function updateCharCounter() {
    const textarea = document.getElementById('messageText');
    const counter = document.getElementById('charCounter');
    const length = textarea.value.length;
    counter.textContent = `${length}/4096`;
}

function updateButtonState() {
    const textarea = document.getElementById('messageText');
    const button = document.getElementById('sendMessageBtn');
    const hasText = textarea.value.trim().length > 0;
    
    if (hasText && !isLoading) {
        button.classList.add('active');
        button.disabled = false;
    } else {
        button.classList.remove('active');
        button.disabled = true;
    }
}

function showLoading(show) {
    // Загрузка уже отображается через спиннеры в HTML
}

function showError(message) {
    tg.showPopup({
        title: 'Ошибка',
        message: message,
        buttons: [{ type: 'cancel' }]
    });
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
    // Переключение табов
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const tab = e.target.dataset.tab;
            
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(`${tab}-tab`).classList.add('active');
        });
    });
    
    const textarea = document.getElementById('messageText');
    const sendBtn = document.getElementById('sendMessageBtn');
    
    textarea.addEventListener('input', () => {
        updateCharCounter();
        updateButtonState();
    });
    
    sendBtn.addEventListener('click', () => {
        const text = textarea.value.trim();
        if (text && !isLoading) {
            sendMessage(text);
        }
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

document.addEventListener('DOMContentLoaded', init);
