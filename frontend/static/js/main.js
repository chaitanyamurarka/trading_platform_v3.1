// chaitanyamurarka/trading_platform_v3.1/trading_platform_v3.1-66a7995b817d40714564deabfdf0ca0ce8992874/frontend/static/js/main.js
document.addEventListener('DOMContentLoaded', () => {
    const chartContainer = document.getElementById('chartContainer');
    const loadChartBtn = document.getElementById('loadChartBtn');
    const exchangeSelect = document.getElementById('exchange');
    const symbolSelect = document.getElementById('symbol');
    const intervalSelect = document.getElementById('interval');
    const startTimeInput = document.getElementById('start_time');
    const endTimeInput = document.getElementById('end_time');
    const themeToggle = document.getElementById('theme-toggle');
    const dataSummaryElement = document.getElementById('dataSummary');
    const loadingIndicator = document.getElementById('loadingIndicator');

    let allChartData = [];
    let allVolumeData = [];
    let currentlyFetching = false;
    let oldestDataTimestamp = null;
    let allDataLoaded = false; // Flag to track if all data is loaded

    let mainChart = null;
    let candleSeries = null;
    let volumeSeries = null;
    let sessionToken = null; // To store the session token
    let heartbeatIntervalId = null; // To store the interval ID for the heartbeat

    // Function to start the session and heartbeat
    async function startSession() {
        try {
            const sessionData = await initiateSession();
            if (sessionData && sessionData.session_token) {
                sessionToken = sessionData.session_token;
                console.log(`Session started with token: ${sessionToken}`);
                showToast(`Session started.`, 'info');

                if (heartbeatIntervalId) clearInterval(heartbeatIntervalId);

                heartbeatIntervalId = setInterval(async () => {
                    if (sessionToken) {
                        try {
                            const heartbeatStatus = await sendHeartbeat(sessionToken);
                            if (heartbeatStatus.status !== 'ok') {
                                console.error('Heartbeat failed:', heartbeatStatus.message);
                                clearInterval(heartbeatIntervalId);
                                showToast('Session expired. Please reload the page.', 'error');
                            } else {
                                console.log('Heartbeat sent successfully.');
                            }
                        } catch (e) {
                            console.error('Error sending heartbeat:', e);
                            clearInterval(heartbeatIntervalId);
                            showToast('Connection lost. Please reload.', 'error');
                        }
                    }
                }, 60000);

            } else {
                throw new Error("Invalid session data received from server.");
            }
        } catch (error) {
            console.error('Failed to initiate session:', error);
            showToast('Could not start a session. Please check connection and reload.', 'error');
        }
    }

    const getChartTheme = () => {
        const isDarkMode = document.documentElement.getAttribute('data-theme') === 'dark';
        return {
            layout: {
                background: { type: 'solid', color: isDarkMode ? '#1d232a' : '#ffffff' },
                textColor: isDarkMode ? '#a6adba' : '#1f2937',
                fontFamily: 'Inter, sans-serif',
            },
            grid: {
                vertLines: { color: isDarkMode ? '#2a323c' : '#e5e7eb' },
                horzLines: { color: isDarkMode ? '#2a323c' : '#e5e7eb' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: isDarkMode ? '#2a323c' : '#e5e7eb',
            },
            timeScale: {
                borderColor: isDarkMode ? '#2a323c' : '#e5e7eb',
                timeVisible: true,
                secondsVisible: ['1s', '5s', '10s', '15s', '30s', '45s'].includes(intervalSelect.value),
            },
        };
    };
    
    function initializeCharts() {
        if (mainChart) mainChart.remove();
        mainChart = LightweightCharts.createChart(chartContainer, getChartTheme());
        candleSeries = mainChart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: '#10b981', downColor: '#ef4444',
            borderVisible: false, wickUpColor: '#10b981', wickDownColor: '#ef4444',
        });
        volumeSeries = mainChart.addSeries(LightweightCharts.HistogramSeries, {
            color: '#9ca3af', priceFormat: { type: 'volume' }, priceScaleId: '', 
        });
        mainChart.priceScale('').applyOptions({ scaleMargins: { top: 0.7, bottom: 0 } });

        mainChart.timeScale().subscribeVisibleLogicalRangeChange(async (newVisibleRange) => {
            if (!newVisibleRange) {
                return;
            }

            if (allDataLoaded && newVisibleRange.from < 0) {
                const currentRange = mainChart.timeScale().getVisibleLogicalRange();
                if (currentRange) {
                    const rangeWidth = currentRange.to - currentRange.from;
                    mainChart.timeScale().setVisibleLogicalRange({
                        from: 0,
                        to: rangeWidth,
                    });
                }
                return;
            }

            if (currentlyFetching || !oldestDataTimestamp || allDataLoaded) {
                return;
            }

            const lazyLoadThreshold = Math.max(50, Math.floor(allChartData.length * 0.05));

            if (newVisibleRange.from < lazyLoadThreshold) { 
                // =================== FIX START ===================
                // Append 'Z' to the datetime-local string to interpret it as UTC, preventing timezone shifts.
                const userSelectedStartDate = new Date(startTimeInput.value + 'Z');
                // =================== FIX END ===================

                const currentOldestDate = new Date(oldestDataTimestamp * 1000);

                if (currentOldestDate <= userSelectedStartDate) {
                    allDataLoaded = true;
                    return;
                }
                
                const nextStartTime = userSelectedStartDate;
                const nextEndTime = new Date(currentOldestDate.getTime() - 1000);


                if (nextStartTime >= nextEndTime) {
                    allDataLoaded = true;
                    return;
                }

                const nextStartTimeStr = nextStartTime.toISOString().slice(0, 16);
                const nextEndTimeStr = nextEndTime.toISOString().slice(0, 16);
                
                showToast('Loading older data...', 'info');
                await fetchAndApplyData(nextStartTimeStr, nextEndTimeStr, true);
            }
        });
    }

    async function fetchAndApplyData(startTime, endTime, prepending = false) {
        if (currentlyFetching) {
            console.log("Already fetching data, please wait.");
            return;
        }
        currentlyFetching = true;
        if (loadingIndicator) loadingIndicator.style.display = 'flex';

        const exchange = exchangeSelect.value;
        const token = symbolSelect.value;
        const interval = intervalSelect.value;

        if (startTime.length === 16) startTime += ':00';
        if (endTime.length === 16) endTime += ':00';

        const apiUrl = getHistoricalDataUrl(sessionToken, exchange, token, interval, startTime, endTime);

        try {
            const response = await fetch(apiUrl);
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: response.statusText }));
                throw new Error(`HTTP error ${response.status}: ${errorData.detail || 'Failed to fetch data'}`);
            }
            
            const responseData = await response.json();

            if (!responseData || !Array.isArray(responseData.candles) || responseData.candles.length === 0) {
                const message = responseData.message || 'No historical data available for this range.';
                showToast(message, 'info');
                if (prepending) {
                    console.log("Lazy loading complete: No more older candles returned.");
                    allDataLoaded = true;
                }
                return;
            }

            if (prepending && !responseData.is_partial) {
                console.log("Lazy loading complete: Server indicated end of data.");
                allDataLoaded = true;
            }

            const candleData = responseData.candles;
            const chartFormattedData = candleData.map(item => ({
                time: item.unix_timestamp, open: item.open, high: item.high, low: item.low, close: item.close,
            }));
            const volumeFormattedData = candleData.map(item => ({
                time: item.unix_timestamp, value: item.volume,
                color: item.close > item.open ? 'rgba(16, 185, 129, 0.5)' : 'rgba(239, 68, 68, 0.5)',
            }));

            if (prepending) {
                allChartData = [...chartFormattedData, ...allChartData];
                allVolumeData = [...volumeFormattedData, ...allVolumeData];
            } else {
                allChartData = chartFormattedData;
                allVolumeData = volumeFormattedData;
                if (!responseData.is_partial) {
                    allDataLoaded = true;
                }
            }
            
            if(chartFormattedData.length > 0) {
                oldestDataTimestamp = chartFormattedData[0].time;
            }

            if (candleSeries) candleSeries.setData(allChartData);
            if (volumeSeries) volumeSeries.setData(allVolumeData);
            
            if (!prepending) {
                const dataSize = allChartData.length;
                if (dataSize > 0) {
                    const numberOfBarsToShow = 100;
                    const visibleFrom = Math.max(0, dataSize - numberOfBarsToShow);
                    const visibleTo = dataSize - 1;
                    mainChart.timeScale().setVisibleLogicalRange({ from: visibleFrom, to: visibleTo });
                } else {
                    mainChart.timeScale().fitContent();
                }
            }

            updateDataSummary(allChartData.length > 0 ? allChartData[allChartData.length - 1] : null, token, exchange, interval);
            showToast(`Chart data loaded. Total points: ${allChartData.length}`, 'success');

        } catch (error) {
            console.error('Failed to fetch chart data:', error);
            showToast(`Error: ${error.message}`, 'error');
            dataSummaryElement.textContent = `Error loading data: ${error.message}`;
        } finally {
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            currentlyFetching = false;
        }
    }
    
    function updateDataSummary(latestData, symbol, exchange, interval) {
        if (!latestData) {
            dataSummaryElement.innerHTML = 'No data to summarize.';
            return;
        }
        const change = latestData.close - latestData.open;
        const changePercent = (latestData.open === 0) ? 0 : (change / latestData.open) * 100;
        const changeClass = change >= 0 ? 'text-success' : 'text-error';
        const dateObj = new Date(latestData.time * 1000);
        const formattedDate = `${dateObj.getDate().toString().padStart(2, '0')}/${(dateObj.getMonth() + 1).toString().padStart(2, '0')}/${dateObj.getFullYear()} ${dateObj.getHours().toString().padStart(2, '0')}:${dateObj.getMinutes().toString().padStart(2, '0')}:${dateObj.getSeconds().toString().padStart(2, '0')}`;
        const lastVolumeData = allVolumeData.find(d => d.time === latestData.time);
        const volume = lastVolumeData ? lastVolumeData.value : 'N/A';
        dataSummaryElement.innerHTML = `
            <strong>${symbol} (${exchange}) - ${interval}</strong><br>
            Last: O: ${latestData.open.toFixed(2)} H: ${latestData.high.toFixed(2)} L: ${latestData.low.toFixed(2)} C: ${latestData.close.toFixed(2)} V: ${volume ? volume.toLocaleString() : 'N/A'}<br>
            Change: <span class="${changeClass}">${change.toFixed(2)} (${changePercent.toFixed(2)}%)</span><br>
            Time: ${formattedDate}
        `;
    }

    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('chartTheme', theme);
        if (mainChart) mainChart.applyOptions(getChartTheme());
    }

    themeToggle.addEventListener('click', () => {
        const newTheme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        applyTheme(newTheme);
    });

    const savedTheme = localStorage.getItem('chartTheme') || 'light';
    applyTheme(savedTheme);
    themeToggle.classList.toggle('swap-active', savedTheme === 'dark');

    function setDefaultDateTime() {
        const now = new Date();
        const oneMonthAgo = new Date(now);
        oneMonthAgo.setMonth(now.getMonth() - 1);
        oneMonthAgo.setHours(0, 0, 0, 0); 
        const endDateTime = new Date(now);
        endDateTime.setHours(0,0,0,0); 
        const formatForInput = (date) => `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}T00:00`;
        startTimeInput.value = formatForInput(oneMonthAgo);
        endTimeInput.value = formatForInput(endDateTime);
    }
    
    function showToast(message, type = 'info') {
        const toastContainer = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `alert alert-${type} shadow-lg animate-pulse`;
        toast.style.animationDuration = '2s';
        let iconHtml = '';
        if (type === 'success') iconHtml = '<svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>';
        else if (type === 'error') iconHtml = '<svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>';
        else if (type === 'warning') iconHtml = '<svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>';
        else iconHtml = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" class="stroke-info shrink-0 w-6 h-6"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>';
        toast.innerHTML = `${iconHtml}<span>${message}</span>`;
        toastContainer.appendChild(toast);
        setTimeout(() => {
            toast.classList.remove('animate-pulse');
            toast.style.opacity = '0';
            toast.style.transition = 'opacity 0.5s ease-out';
            setTimeout(() => toast.remove(), 500);
        }, 3000);
    }
    
    async function loadInitialChart() {
        if (!sessionToken) {
            showToast('Waiting for session to start...', 'info');
            return;
        }

        const startTimeStr = startTimeInput.value;
        const endTimeStr = endTimeInput.value;

        if (!startTimeStr || !endTimeStr) {
            showToast('Start Time and End Time are required.', 'error');
            return;
        }
        
        allDataLoaded = false;
        allChartData = [];
        allVolumeData = [];
        oldestDataTimestamp = null;

        await fetchAndApplyData(startTimeStr, endTimeStr, false);
    }

    loadChartBtn.addEventListener('click', loadInitialChart);

    window.addEventListener('resize', () => { if (mainChart) mainChart.resize(chartContainer.clientWidth, chartContainer.clientHeight); });

    // --- Final Initialization Sequence ---
    setDefaultDateTime();
    initializeCharts();
    startSession(); 
});