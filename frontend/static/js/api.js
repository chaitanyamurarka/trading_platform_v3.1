// static/js/api.js

// Determine the base URL for the API
let API_PROTOCOL = window.location.protocol;
let API_HOSTNAME = window.location.hostname;
const API_PORT = '8000'; // The port your FastAPI backend is running on

// If opened as a local file, default to http://localhost for the API
if (API_PROTOCOL === 'file:') {
    API_PROTOCOL = 'http:';
    API_HOSTNAME = 'localhost'; // Or '127.0.0.1'
    console.warn('Frontend is opened as a local file. API calls will be directed to http://localhost:8000.');
}

const API_BASE_URL = `${API_PROTOCOL}//${API_HOSTNAME}:${API_PORT}`;

// New function to get a session token
function initiateSession() {
    return fetch(`${API_BASE_URL}/utils/session/initiate`).then(res => res.json());
}

// New function for heartbeat
function sendHeartbeat(token) {
    return fetch(`${API_BASE_URL}/utils/session/heartbeat`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ session_token: token }),
    }).then(res => res.json());
}


/**
 * Constructs the URL for fetching historical data.
 * @param {string} sessionToken - The unique session token for the user.
 * @param {string} exchange - The exchange name.
 * @param {string} token - The asset symbol/token.
 * @param {string} interval - The data interval.
 * @param {string} startTime - The start time in ISO format.
 * @param {string} endTime - The end time in ISO format.
 * @returns {string} The full API URL for historical data.
 */
function getHistoricalDataUrl(sessionToken, exchange, token, interval, startTime, endTime) {
    const params = new URLSearchParams({
        session_token: sessionToken,
        exchange: exchange,
        token: token,
        interval: interval,
        start_time: startTime,
        end_time: endTime
    });
    return `${API_BASE_URL}/historical/?${params.toString()}`;
}

// You can add more functions here for other API endpoints as needed
// Example:
// function getSomeOtherDataUrl(params) {
//     return `${API_BASE_URL}/other-endpoint/?${new URLSearchParams(params).toString()}`;
// }