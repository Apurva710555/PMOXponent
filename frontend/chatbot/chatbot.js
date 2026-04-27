/**
 * PMOXponent Chatbot Component Logic
 * Maintains conversation state using Databricks Genie and localStorage.
 */

(function() {
    const CHATBOT_STORAGE_KEY = 'pmox_genie_conversation_id';

    /**
     * Toggles the chat overlay visibility.
     */
    window.togglePMOXChat = function() {
        const overlay = document.getElementById('pmoxChatOverlay');
        if (!overlay) return;

        overlay.classList.toggle('open');
        
        if (overlay.classList.contains('open')) {
            const input = document.getElementById('pmoxInput');
            if (input) {
                setTimeout(() => input.focus(), 300);
            }
        }
    };

    /**
     * Sends the user's message to the backend and renders the Genie response.
     */
    window.pmoxSend = async function() {
        const input = document.getElementById('pmoxInput');
        const messages = document.getElementById('pmoxMessages');
        if (!input || !messages) return;

        const text = input.value.trim();
        if (!text) return;
        
        input.value = '';

        // Render User Message
        const userMsg = document.createElement('div');
        userMsg.className = 'pmox-msg pmox-user';
        userMsg.textContent = text;
        messages.appendChild(userMsg);
        messages.scrollTop = messages.scrollHeight;

        // Render Typing Indicator
        const typingIndicator = document.createElement('div');
        typingIndicator.className = 'pmox-msg pmox-bot pmox-typing';
        typingIndicator.innerHTML = '<span></span><span></span><span></span>';
        messages.appendChild(typingIndicator);
        messages.scrollTop = messages.scrollHeight;

        try {
            // Retrieve persisted conversation ID (if any)
            const conversationId = localStorage.getItem(CHATBOT_STORAGE_KEY);

            const response = await fetch('/api/chatbot/ask', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    question: text,
                    conversation_id: conversationId
                })
            });

            const data = await response.json();

            // Cleanup typing indicator
            typingIndicator.classList.remove('pmox-typing');
            typingIndicator.innerHTML = '';

            if (data.status === 'success') {
                // Parse basic markdown from Genie into HTML
                let formattedAnswer = data.answer
                    .replace(/\n\n/g, '<br><br>')
                    .replace(/\n/g, '<br>')
                    .replace(/\*\*(.*?)\*\*/g, '<b>$1</b>')
                    .replace(/\*(.*?)\*/g, '<i>$1</i>');
                    
                typingIndicator.innerHTML = formattedAnswer;
                
                // Persist the conversation ID to maintain context on refresh
                if (data.conversation_id) {
                    localStorage.setItem(CHATBOT_STORAGE_KEY, data.conversation_id);
                }
            } else {
                typingIndicator.innerHTML = 'Oops! ' + (data.message || 'I encountered an issue.');
                typingIndicator.classList.add('error-msg'); // Optional styling
            }

        } catch (error) {
            console.error('Chatbot API Error:', error);
            typingIndicator.classList.remove('pmox-typing');
            typingIndicator.innerHTML = '';
            typingIndicator.innerHTML = 'Server unreachable. Please try again later.';
        }

        messages.scrollTop = messages.scrollHeight;
    };
})();
