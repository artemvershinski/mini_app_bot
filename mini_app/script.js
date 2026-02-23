// Глобальные переменные
let tg = window.Telegram.WebApp;
let userData = null;
let isAdmin = false;
let currentMessageId = null;

// Инициализация при загрузке
tg.expand();
tg.enableClosingConfirmation();

// Цвета под тему Telegram
tg.setHeaderColor('#232323');
tg.setBackgroundColor('#232323');

// Главная функция инициализации
async function init() {
    try {
        // Показываем загрузку
        showLoading(true);
        
        // Аутентификация
        const authResult = await authenticate();
        
        if (authResult.ok) {
            userData = authResult.user;
            isAdmin = userData.is_admin;
            
            // Обновляем UI
            updateUserInfo();
            
            // Настраиваем интерфейс под роль
            setupInterface();
            
            // Загружаем данные
            if (isAdmin) {
                await loadInboxMessages();
                await loadStats();
            } else {
                await loadUserMessages();
            }
            
            // Настраиваем обработчики
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

// Аутентификация
async function authenticate() {
    const initData = tg.initData;
    
    if (!initData) {
        return { ok: false, error: 'No init data' };
    }
    
    try {
        const response = await fetch('/api/auth', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ initData })
        });
        
        return await response.json();
    } catch (error) {
        console.error('Auth error:', error);
        return { ok: false, error: error.message };
    }
}

// Обновление информации о пользователе
function updateUserInfo() {
    if (userData) {
        const userName = document.getElementById('userName');
        userName.textContent = userData.first_name || userData.username || 'User';
        
        if (!isAdmin && userData.unanswered > 0) {
            const badge = document.getElementById('unansweredBadge');
            badge.textContent = userData.unanswered;
            badge.classList.remove('hidden');
        }
    }
}

// Настройка интерфейса под роль
function setupInterface() {
    const statsTab = document.querySelector('[data-tab="stats"]');
    const inputContainer = document.getElementById('messageInputContainer');
    
    if (isAdmin) {
        // Админ видит все табы
        statsTab.classList.remove('hidden');
        // Админ не видит поле ввода
        inputContainer.classList.add('hidden');
    } else {
        // Обычный пользователь не видит статистику
        statsTab.classList.add('hidden');
        // Пользователь видит поле ввода
        inputContainer.classList.remove('hidden');
        
        // Переключаем на вкладку отправленных
        document.querySelector('[data-tab="sent"]').classList.add('active');
        document.querySelector('[data-tab="inbox"]').classList.remove('active');
        document.getElementById('sent-tab').classList.add('active');
        document.getElementById('inbox-tab').classList.remove('active');
    }
}

// Загрузка сообщений для обычного пользователя
async function loadUserMessages() {
    try {
        const response = await fetch('/api/messages', {
            headers: {
                'X-Telegram-Init-Data': tg.initData
            }
        });
        
        const data = await response.json();
        
        if (data.messages) {
            displayUserMessages(data.messages);
        }
    } catch (error) {
        console.error('Load messages error:', error);
        showError('Не удалось загрузить сообщения');
    }
}

// Отображение сообщений пользователя
function displayUserMessages(messages) {
    const container = document.getElementById('sentMessages');
    
    if (!messages || messages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📭</div>
                <h3>Нет сообщений</h3>
                <p>Отправьте свое первое сообщение</p>
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
        const statusText = msg.is_answered ? '✓ Отвечено' : '⏳ Ожидает ответа';
        
        html += `
            <div class="message-card" data-message-id="${msg.message_id}">
                <div class="message-header">
                    <span class="message-id">#${msg.message_id}</span>
                    <span class="message-time">${timeStr}</span>
                </div>
                
                <div class="message-status ${statusClass}">
                    ${statusText}
                </div>
                
                <div class="message-text">
                    ${escapeHtml(msg.text || msg.caption || 'Медиа-сообщение')}
                </div>
                
                <div class="message-footer">
                    <span>📱 Отправлено</span>
                </div>
                
                ${msg.is_answered ? `
                    <div class="answer-badge">
                        <div class="answer-header">
                            <span>💬 Ответ:</span>
                        </div>
                        <div class="answer-text">
                            ${escapeHtml(msg.answer_text || 'Ответ получен')}
                        </div>
                        <div class="answer-meta">
                            ${msg.answered_by_name ? `От: ${msg.answered_by_name}` : ''}
                            ${msg.answered_at ? ` • ${new Date(msg.answered_at).toLocaleString('ru-RU', {
                                hour: '2-digit',
                                minute: '2-digit'
                            })}` : ''}
                        </div>
                    </div>
                ` : ''}
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// Загрузка входящих для админа
async function loadInboxMessages() {
    try {
        const response = await fetch('/api/admin/messages', {
            headers: {
                'X-Telegram-Init-Data': tg.initData
            }
        });
        
        const data = await response.json();
        
        if (data.messages) {
            displayInboxMessages(data.messages);
        }
    } catch (error) {
        console.error('Load inbox error:', error);
        showError('Не удалось загрузить входящие');
    }
}

// Отображение входящих для админа
function displayInboxMessages(messages) {
    const container = document.getElementById('inboxMessages');
    
    if (!messages || messages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📨</div>
                <h3>Нет сообщений</h3>
                <p>Пока никто не написал</p>
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
        const statusText = msg.is_answered ? '✓ Отвечено' : '⏳ Требует ответа';
        
        html += `
            <div class="message-card" data-message-id="${msg.message_id}">
                <div class="message-header">
                    <span class="message-id">#${msg.message_id}</span>
                    <span class="message-time">${timeStr}</span>
                </div>
                
                <div class="message-header" style="margin-top: 0; border-bottom: none;">
                    <span class="message-id" style="color: var(--text-secondary); font-size: 13px;">
                        От: ${escapeHtml(msg.first_name || msg.username || 'User')} 
                        ${msg.username ? `(@${msg.username})` : ''}
                    </span>
                    <span class="message-id" style="color: var(--text-secondary); font-size: 13px;">
                        ID: ${msg.user_id}
                    </span>
                </div>
                
                <div class="message-status ${statusClass}">
                    ${statusText}
                </div>
                
                <div class="message-text">
                    ${escapeHtml(msg.text || msg.caption || 'Медиа-сообщение')}
                </div>
                
                ${!msg.is_answered ? `
                    <button class="reply-btn" onclick="openReplyModal(${msg.message_id}, '${escapeHtml(msg.text || msg.caption || 'Медиа-сообщение').replace(/'/g, "\\'")}')">
                        <span>✏️</span> Ответить
                    </button>
                ` : `
                    <div class="answer-badge">
                        <div class="answer-header">
                            <span>✅ Отвечено:</span>
                        </div>
                        <div class="answer-text">
                            ${escapeHtml(msg.answer_text || 'Ответ отправлен')}
                        </div>
                        <div class="answer-meta">
                            ${msg.answered_by_name ? `От: ${msg.answered_by_name}` : ''}
                        </div>
                    </div>
                `}
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// Загрузка статистики для админа
async function loadStats() {
    try {
        const response = await fetch('/api/admin/stats', {
            headers: {
                'X-Telegram-Init-Data': tg.initData
            }
        });
        
        const stats = await response.json();
        
        // Загружаем пользователей
        const usersResponse = await fetch('/api/admin/users', {
            headers: {
                'X-Telegram-Init-Data': tg.initData
            }
        });
        
        const usersData = await usersResponse.json();
        
        displayStats(stats, usersData.users || []);
    } catch (error) {
        console.error('Load stats error:', error);
    }
}

// Отображение статистики
function displayStats(stats, users) {
    const container = document.getElementById('statsContent');
    
    const usersHtml = users.slice(0, 5).map(user => `
        <div class="user-card">
            <div class="user-info">
                <h4>${escapeHtml(user.first_name || 'User')} ${user.is_banned ? '🚫' : ''}</h4>
                <div class="user-meta">
                    ${user.username ? `@${user.username}` : `ID: ${user.user_id}`}
                </div>
            </div>
            <div class="user-stats">
                <div class="user-messages">${user.messages_count || 0}</div>
                <div class="user-meta">сообщений</div>
                ${user.unanswered_count > 0 ? 
                    `<div class="status-waiting" style="margin-top: 4px;">${user.unanswered_count} ожидают</div>` : 
                    ''}
            </div>
        </div>
    `).join('');
    
    container.innerHTML = `
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Всего сообщений</div>
                <div class="stat-value">${stats.total_messages || 0}</div>
                <div class="stat-trend">+${stats.messages_today || 0} сегодня</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Пользователей</div>
                <div class="stat-value">${stats.total_users || users.length}</div>
                <div class="stat-trend">${stats.active_users_today || 0} активны сегодня</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Отвечено</div>
                <div class="stat-value">${stats.answers_sent || 0}</div>
                <div class="stat-trend">из ${stats.total_messages || 0}</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Заблокировано</div>
                <div class="stat-value">${stats.bans_issued || 0}</div>
                <div class="stat-trend">пользователей</div>
            </div>
        </div>
        
        <h3 style="margin: 24px 0 16px; color: var(--text-secondary);">Активные пользователи</h3>
        
        <div class="users-list">
            ${usersHtml}
        </div>
    `;
}

// Отправка сообщения
async function sendMessage(text) {
    try {
        const response = await fetch('/api/send', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                initData: tg.initData,
                text: text
            })
        });
        
        const result = await response.json();
        
        if (result.ok) {
            // Показываем успех
            tg.showPopup({
                title: 'Успешно',
                message: `Сообщение #${result.message_id} отправлено!`,
                buttons: [{ type: 'ok' }]
            });
            
            // Очищаем поле
            document.getElementById('messageText').value = '';
            updateCharCounter();
            
            // Обновляем список
            await loadUserMessages();
        } else {
            tg.showPopup({
                title: 'Ошибка',
                message: result.error || 'Не удалось отправить сообщение',
                buttons: [{ type: 'cancel' }]
            });
        }
    } catch (error) {
        console.error('Send error:', error);
        tg.showPopup({
            title: 'Ошибка',
            message: 'Не удалось отправить сообщение',
            buttons: [{ type: 'cancel' }]
        });
    }
}

// Отправка ответа (для админов)
async function sendReply(messageId, answer) {
    try {
        const response = await fetch('/api/admin/reply', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                initData: tg.initData,
                message_id: messageId,
                answer: answer
            })
        });
        
        const result = await response.json();
        
        if (result.ok) {
            tg.showPopup({
                title: 'Успешно',
                message: `Ответ на #${messageId} отправлен!`,
                buttons: [{ type: 'ok' }]
            });
            
            closeModal();
            await loadInboxMessages();
        } else {
            tg.showPopup({
                title: 'Ошибка',
                message: result.error || 'Не удалось отправить ответ',
                buttons: [{ type: 'cancel' }]
            });
        }
    } catch (error) {
        console.error('Reply error:', error);
        tg.showPopup({
            title: 'Ошибка',
            message: 'Не удалось отправить ответ',
            buttons: [{ type: 'cancel' }]
        });
    }
}

// Открытие модалки ответа
function openReplyModal(messageId, originalText) {
    currentMessageId = messageId;
    document.getElementById('replyMsgId').textContent = messageId;
    document.getElementById('originalMessagePreview').innerHTML = `
        <div style="background: var(--bg-tertiary); padding: 12px; border-radius: var(--radius-base); margin-bottom: 12px; font-size: 14px; color: var(--text-secondary);">
            ${escapeHtml(originalText)}
        </div>
    `;
    document.getElementById('replyModal').classList.add('active');
    document.getElementById('replyText').focus();
}

// Закрытие модалки
function closeModal() {
    document.getElementById('replyModal').classList.remove('active');
    document.getElementById('replyText').value = '';
    currentMessageId = null;
}

// Обновление счетчика символов
function updateCharCounter() {
    const textarea = document.getElementById('messageText');
    const counter = document.getElementById('charCounter');
    const length = textarea.value.length;
    counter.textContent = `${length}/4096`;
    
    if (length >= 4000) {
        counter.style.color = 'var(--status-waiting)';
    } else {
        counter.style.color = 'var(--text-tertiary)';
    }
}

// Показ/скрытие загрузки
function showLoading(show) {
    // Реализация зависит от UI
}

// Показ ошибки
function showError(message) {
    tg.showPopup({
        title: 'Ошибка',
        message: message,
        buttons: [{ type: 'cancel' }]
    });
}

// Экранирование HTML
function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// Настройка обработчиков событий
function setupEventListeners() {
    // Переключение табов
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            const tab = e.target.dataset.tab;
            
            // Обновляем активный класс
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            
            // Показываем соответствующий таб
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(`${tab}-tab`).classList.add('active');
            
            // Загружаем данные если нужно
            if (tab === 'inbox' && isAdmin) {
                await loadInboxMessages();
            } else if (tab === 'sent' && !isAdmin) {
                await loadUserMessages();
            } else if (tab === 'stats' && isAdmin) {
                await loadStats();
            }
        });
    });
    
    // Отправка сообщения
    document.getElementById('sendMessageBtn').addEventListener('click', () => {
        const text = document.getElementById('messageText').value.trim();
        if (text) {
            sendMessage(text);
        } else {
            tg.showPopup({
                title: 'Ошибка',
                message: 'Введите текст сообщения',
                buttons: [{ type: 'ok' }]
            });
        }
    });
    
    // Отправка ответа
    document.getElementById('sendReplyBtn').addEventListener('click', () => {
        const answer = document.getElementById('replyText').value.trim();
        if (answer && currentMessageId) {
            sendReply(currentMessageId, answer);
        }
    });
    
    // Счетчик символов
    document.getElementById('messageText').addEventListener('input', updateCharCounter);
    
    // Enter для отправки (Cmd+Enter)
    document.getElementById('messageText').addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            document.getElementById('sendMessageBtn').click();
        }
    });
    
    document.getElementById('replyText').addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            document.getElementById('sendReplyBtn').click();
        }
    });
    
    // Закрытие модалки по ESC
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeModal();
        }
    });
    
    // Закрытие модалки по клику вне
    document.getElementById('replyModal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('replyModal')) {
            closeModal();
        }
    });
}

// Запуск при загрузке
document.addEventListener('DOMContentLoaded', init);