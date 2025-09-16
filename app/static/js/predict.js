/**
 * JavaScript cho trang Dự Đoán Giá Xe
 * File: predict.js
 */

document.addEventListener('DOMContentLoaded', function() {
    // Lấy các phần tử form
    const brandSelect = document.getElementById('brand');
    const modelSelect = document.getElementById('model');
    const carTypeSelect = document.getElementById('car_type');
    
    // Thiết lập form validation
    const form = document.getElementById('predictionForm');
    if (form) {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    }
    
    // Xử lý sự kiện khi chọn hãng xe
    if (brandSelect) {
        brandSelect.addEventListener('change', function() {
            // Reset các select phụ thuộc
            resetSelect(modelSelect, 'Chọn dòng xe');
            resetSelect(carTypeSelect, 'Chọn kiểu dáng');
            
            const brandId = this.value;
            if (brandId) {
                // Enable model select
                modelSelect.disabled = false;
                
                // Fetch models for the selected brand
                fetch(`/api/get-models/${brandId}`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.success) {
                            // Populate model select
                            data.models.forEach(model => {
                                const option = document.createElement('option');
                                option.value = model.id;
                                option.textContent = model.name;
                                modelSelect.appendChild(option);
                            });
                            
                            // If no models available
                            if (data.models.length === 0) {
                                const option = document.createElement('option');
                                option.value = '';
                                option.textContent = 'Không có dòng xe nào';
                                modelSelect.appendChild(option);
                            }
                        } else {
                            console.error('Error fetching models:', data.error);
                            showAlert('Không thể lấy danh sách dòng xe. Vui lòng thử lại.', 'danger');
                        }
                    })
                    .catch(error => {
                        console.error('API error:', error);
                        showAlert('Lỗi kết nối API. Vui lòng thử lại sau.', 'danger');
                    });
            } else {
                // Disable model select if no brand is selected
                modelSelect.disabled = true;
                carTypeSelect.disabled = true;
            }
        });
    }
    
    // Xử lý sự kiện khi chọn dòng xe
    if (modelSelect) {
        modelSelect.addEventListener('change', function() {
            // Reset car type select
            resetSelect(carTypeSelect, 'Chọn kiểu dáng');
            
            const modelId = this.value;
            if (modelId) {
                // Enable car type select
                carTypeSelect.disabled = false;
                
                // Fetch car types for the selected model
                fetch(`/api/get-car-types/${modelId}`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.success) {
                            // Populate car type select
                            data.car_types.forEach(type => {
                                const option = document.createElement('option');
                                option.value = type.id;
                                option.textContent = type.name;
                                carTypeSelect.appendChild(option);
                            });
                            
                            // If no car types available, add some default options
                            if (data.car_types.length === 0) {
                                const defaultTypes = ['Sedan', 'SUV', 'Hatchback', 'Coupe', 'Pickup'];
                                defaultTypes.forEach((type, index) => {
                                    const option = document.createElement('option');
                                    option.value = `default_${index}`;
                                    option.textContent = type;
                                    carTypeSelect.appendChild(option);
                                });
                            }
                        } else {
                            console.error('Error fetching car types:', data.error);
                            
                            // Add default car types
                            const defaultTypes = ['Sedan', 'SUV', 'Hatchback', 'Coupe', 'Pickup'];
                            defaultTypes.forEach((type, index) => {
                                const option = document.createElement('option');
                                option.value = `default_${index}`;
                                option.textContent = type;
                                carTypeSelect.appendChild(option);
                            });
                        }
                    })
                    .catch(error => {
                        console.error('API error:', error);
                        
                        // Add default car types on error
                        const defaultTypes = ['Sedan', 'SUV', 'Hatchback', 'Coupe', 'Pickup'];
                        defaultTypes.forEach((type, index) => {
                            const option = document.createElement('option');
                            option.value = `default_${index}`;
                            option.textContent = type;
                            carTypeSelect.appendChild(option);
                        });
                    });
            } else {
                // Disable car type select if no model is selected
                carTypeSelect.disabled = true;
            }
        });
    }
    
    // Format số km đã đi
    const mileageInput = document.getElementById('mileage');
    if (mileageInput) {
        mileageInput.addEventListener('blur', function() {
            const value = this.value.trim();
            if (value && !isNaN(value)) {
                const formatted = parseInt(value).toLocaleString('vi-VN');
                this.setAttribute('data-formatted', formatted);
            }
        });
    }
    
    // Hàm tiện ích để reset select
    function resetSelect(selectElement, defaultText) {
        if (!selectElement) return;
        
        // Remove all options except the first one
        while (selectElement.options.length > 1) {
            selectElement.remove(1);
        }
        
        // Update default text if provided
        if (defaultText) {
            selectElement.options[0].textContent = defaultText;
        }
        
        // Reset value
        selectElement.value = '';
        
        // Disable select
        selectElement.disabled = true;
    }
    
    // Hàm hiển thị thông báo
    function showAlert(message, type = 'info') {
        // Create alert element
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
        alertDiv.setAttribute('role', 'alert');
        
        // Set message
        alertDiv.textContent = message;
        
        // Add close button
        const closeButton = document.createElement('button');
        closeButton.className = 'btn-close';
        closeButton.setAttribute('type', 'button');
        closeButton.setAttribute('data-bs-dismiss', 'alert');
        closeButton.setAttribute('aria-label', 'Close');
        alertDiv.appendChild(closeButton);
        
        // Find container to add alert
        const container = document.querySelector('.container');
        if (container) {
            // Insert after header
            const header = container.querySelector('header');
            if (header) {
                container.insertBefore(alertDiv, header.nextSibling);
            } else {
                container.insertBefore(alertDiv, container.firstChild);
            }
            
            // Auto remove after 5 seconds
            setTimeout(() => {
                alertDiv.remove();
            }, 5000);
        }
    }
});