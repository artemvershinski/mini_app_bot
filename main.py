def get_user_inbox(user_id):
    query = "SELECT * FROM messages m WHERE m.user_id = $1 AND m.is_answered = TRUE;"
    return execute_query(query, (user_id,))