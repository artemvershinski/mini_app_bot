let tg = window.Telegram.WebApp;
let userData = null;
let isLoading = false;

tg.expand();

async function init() {
    try {
        console.log('Initializing...');
        
        // Аутентификация
        const response = await fetch('/api/auth?initData=' + encodeURIComponent(tg.initData));
        const authResult = await response.json();
        
        if (authResult.ok) {
            userData = authResult.user;
            
            document.getElementById('userName').textContent = 
                userData.first_name || userData.username || 'User';
            
            if (userData.unanswered > 0) {
                document.getElementById('unansweredBadge').textContent = userData.unanswered;
                document.getElementById('unansweredBadge').classList.remove('hidden');
            }
            
            await loadMessages();
            setupTabs();
            setupEventListeners();
        } else {
            showError('Auth failed');
        }
    } catch (error) {
        console.error('Init error:', error);
        showError('Failed to load');
    }
}

async function loadMessages() {
    try {
        const response = await fetch('/api/messages?initData=' + encodeURIComponent(tg.initData));
        const data = await response.json();
        displayMessages(data.messages || []);
    } catch (error) {
        console.error('Load error:', error);
        displayMessages([]);
    }
}

function displayMessages(messages) {
    const sentContainer = document.getElementById('sentMessages');
    const inboxContainer = document.getElementById('inboxMessages');
    
    const sent = messages.filter(m => !m.is_answered);
    const answered = messages.filter(m => m.is_answered);
    
    // Sent messages
    if (sent.length === 0) {
        sentContainer.innerHTML = '<div class="empty-state"><div class="empty-icon">📭</div><h3>Нет сообщений</h3></div>';
    } else {
        let html = '';
        sent.forEach(m => {
            html += `
                <div class="message-card">
                    <div class="message-header">
                        <span class="message-id">#${m.id}</span>
                        <span class="message-time">${new Date(m.created_at).toLocaleString()}</span>
                    </div>
                    <div class="message-status status-waiting">Ожидает ответа</div>
                    <div class="message-text">${escapeHtml(m.message_text)}</div>
                </div>
            `;
        });
        sentContainer.innerHTML = html;
    }
    
    // Inbox messages
    if (answered.length === 0) {
        inboxContainer.innerHTML = '<div class="empty-state"><div class="empty-icon">📨</div><h3>Нет ответов</h3></div>';
    } else {
        let html = '';
        answered.forEach(m => {
            html += `
                <div class="message-card">
                    <div class="message-header">
                        <span class="message-id">Ответ на #${m.id}</span>
                        <span class="message-time">${new Date(m.answered_at).toLocaleString()}</span>
                    </div>
                    <div class="answer-badge">
                        <div class="answer-header">Администратор:</div>
                        <div class="answer-text">${escapeHtml(m.answer_text || 'Ответ получен')}</div>
                    </div>
                    <div style="margin-top:12px;padding-top:12px;border-top:1px solid rgba(255,255,255,0.1)">
                        <div style="font-size:13px;color:rgba(255,255,255,0.5)">Ваше сообщение:</div>
                        <div style="font-size:14px;color:rgba(255,255,255,0.7)">${escapeHtml(m.message_text)}</div>
                    </div>
                </div>
            `;
        });
        inboxContainer.innerHTML = html;
    }
}

function setupTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(this.dataset.tab + '-tab').classList.add('active');
        });
    });
}

function setupEventListeners() {
    const textarea = document.getElementById('messageText');
    const sendBtn = document.getElementById('sendMessageBtn');
    
    textarea.addEventListener('input', () => {
        document.getElementById('charCounter').textContent = textarea.value.length + '/4096';
        sendBtn.disabled = textarea.value.trim().length === 0 || isLoading;
        sendBtn.classList.toggle('active', textarea.value.trim().length > 0 && !isLoading);
    });
    
    sendBtn.addEventListener('click', sendMessage);
}

async function sendMessage() {
    if (isLoading) return;
    
    const textarea = document.getElementById('messageText');
    const text = textarea.value.trim();
    if (!text) return;
    
    isLoading = true;
    document.getElementById('sendMessageBtn').disabled = true;
    
    try {
        const response = await fetch('/api/send', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData: tg.initData, text})
        });
        
        const result = await response.json();
        
        if (result.ok) {
            textarea.value = '';
            document.getElementById('charCounter').textContent = '0/4096';
            await loadMessages();
            
            if (tg.showPopup) {
                tg.showPopup({
                    title: 'Успешно',
                    message: `Сообщение #${result.message_id} отправлено!`
                });
            }
        } else {
            alert('Error: ' + result.error);
        }
    } catch (error) {
        alert('Send error: ' + error.message);
    } finally {
        isLoading = false;
        document.getElementById('sendMessageBtn').disabled = true;
        document.getElementById('sendMessageBtn').classList.remove('active');
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

function showError(msg) {
    document.getElementById('inboxMessages').innerHTML = `
        <div class="empty-state">
            <div class="empty-icon">⚠️</div>
            <h3>Ошибка</h3>
            <p>${msg}</p>
            <button onclick="location.reload()">Обновить</button>
        </div>
    `;
}

document.addEventListener('DOMContentLoaded', init);
