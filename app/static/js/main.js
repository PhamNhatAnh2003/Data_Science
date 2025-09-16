/**
 * Main JavaScript for the Car Price Prediction application
 */

document.addEventListener('DOMContentLoaded', function() {
    // Poll for crawl status updates if there's an active crawl
    const statusBadges = document.querySelectorAll('.badge');
    statusBadges.forEach(badge => {
        if (badge.textContent === 'running') {
            badge.classList.add('updating');
            
            // Find the closest row to get the log ID
            const row = badge.closest('tr');
            if (row) {
                const logId = row.querySelector('td:first-child').textContent;
                const isProcessingLog = row.querySelector('td:nth-child(2)').textContent.includes('/');
                
                // Set up polling for status updates
                const endpoint = isProcessingLog 
                    ? `/api/processing-status/${logId}`
                    : `/api/crawl-status/${logId}`;
                
                pollStatus(endpoint, badge, row);
            }
        }
    });
    
    // Also check if the latest crawl or processing is running from the cards
    const cardBadges = document.querySelectorAll('.card .badge');
    cardBadges.forEach(badge => {
        if (badge.textContent === 'running') {
            badge.classList.add('updating');
            // TĂNG THỜI GIAN REFRESH: từ 30 giây lên 2 phút
            setTimeout(() => {
                window.location.reload();
            }, 120000); // 2 phút thay vì 30 giây
        }
    });
    
    // Set up form validation
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    });
});

/**
 * Poll an API endpoint for status updates
 * @param {string} endpoint - The API endpoint to poll
 * @param {Element} badge - The badge element to update
 * @param {Element} row - The table row to update
 */
function pollStatus(endpoint, badge, row) {
    let consecutiveErrors = 0;
    const maxErrors = 3;
    
    const interval = setInterval(() => {
        fetch(endpoint)
            .then(response => response.json())
            .then(data => {
                console.log('Polling response:', data); // Debug log
                consecutiveErrors = 0; // Reset error count on success
                
                // Update records count NGAY LẬP TỨC
                if (row) {
                    const recordsCell = row.querySelector('td:nth-child(6)');
                    if (recordsCell) {
                        // Chỉ update nếu có thay đổi để tránh flicker
                        const currentCount = recordsCell.textContent;
                        const newCount = data.records_count || '0';
                        if (currentCount !== newCount) {
                            recordsCell.textContent = newCount;
                            console.log('Updated records count from', currentCount, 'to:', newCount);
                        }
                    }
                    
                    // Update status text nếu có thay đổi
                    if (badge.textContent !== data.status) {
                        badge.textContent = data.status;
                        console.log('Updated status to:', data.status);
                    }
                }
                
                // Chỉ dừng polling khi status thực sự không phải running
                if (data.status !== 'running' && !data.status.startsWith('running-')) {
                    clearInterval(interval);
                    badge.classList.remove('updating');
                    
                    // Update badge class and text
                    badge.classList.remove('bg-warning');
                    badge.classList.add(data.status === 'completed' ? 'bg-success' : 'bg-danger');
                    badge.textContent = data.status;
                    
                    // Update other cells in the row
                    if (row) {
                        // Update end time
                        const endTimeCell = row.querySelector('td:nth-child(4)');
                        if (endTimeCell && data.end_time) {
                            endTimeCell.textContent = data.end_time;
                        }
                        
                        // If it's a processing log, update output file if available
                        if (endpoint.includes('processing-status') && data.output_file) {
                            const outputFileCell = row.querySelector('td:nth-child(3)');
                            if (outputFileCell) {
                                outputFileCell.textContent = data.output_file;
                            }
                        }
                    }
                    
                    // Refresh the page after a longer delay
                    setTimeout(() => {
                        window.location.reload();
                    }, 3000);
                }
            })
            .catch(error => {
                consecutiveErrors++;
                console.error('Error polling status:', error);
                
                // Nếu có quá nhiều lỗi liên tiếp, dừng polling
                if (consecutiveErrors >= maxErrors) {
                    console.error('Too many consecutive errors, stopping polling');
                    clearInterval(interval);
                    badge.classList.remove('updating');
                }
            });
    }, 8000); // Tăng interval lên 8 giây để giảm conflict
}

// Thêm function để manually check stuck crawlers
function checkStuckCrawlers() {
    fetch('/api/check-stuck-crawlers')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                if (data.updated_jobs > 0) {
                    alert(`Đã reset ${data.updated_jobs} crawler jobs bị stuck.`);
                    window.location.reload();
                } else {
                    alert('Không có crawler nào bị stuck.');
                }
            }
        })
        .catch(error => {
            console.error('Error checking stuck crawlers:', error);
            alert('Lỗi khi check stuck crawlers.');
        });
}

// Thêm function để reset crawler cụ thể
function resetCrawler(logId) {
    if (confirm('Bạn có chắc muốn reset crawler này?')) {
        fetch(`/api/reset-crawler/${logId}`)
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    alert(data.message);
                    window.location.reload();
                } else {
                    alert('Lỗi: ' + data.message);
                }
            })
            .catch(error => {
                console.error('Error resetting crawler:', error);
                alert('Lỗi khi reset crawler.');
            });
    }
}

// Thêm function để force refresh records count
function forceUpdateRecords() {
    // Find all running crawlers and update their records count
    const runningBadges = document.querySelectorAll('.badge.bg-warning');
    runningBadges.forEach(badge => {
        if (badge.textContent.includes('running')) {
            const row = badge.closest('tr');
            if (row) {
                const logId = row.querySelector('td:first-child').textContent;
                const isProcessingLog = row.querySelector('td:nth-child(2)').textContent.includes('/');
                
                const endpoint = isProcessingLog 
                    ? `/api/processing-status/${logId}`
                    : `/api/crawl-status/${logId}`;
                
                // Force một lần update
                fetch(endpoint)
                    .then(response => response.json())
                    .then(data => {
                        const recordsCell = row.querySelector('td:nth-child(6)');
                        if (recordsCell) {
                            recordsCell.textContent = data.records_count || '0';
                            console.log(`Force updated records for log ${logId}: ${data.records_count}`);
                        }
                    })
                    .catch(error => {
                        console.error('Error force updating records:', error);
                    });
            }
        }
    });
}

// Auto force update mỗi 15 giây
setInterval(forceUpdateRecords, 15000);