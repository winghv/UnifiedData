import axios from 'axios';

const API_URL = '/api/metrics';

export default {
  getMetrics() {
    return axios.get(API_URL);
  },
  getMetric(id) {
    return axios.get(`${API_URL}/${id}`);
  },
  createMetric(metric) {
    return axios.post(API_URL, metric);
  },
  updateMetric(id, metric) {
    return axios.put(`${API_URL}/${id}`, metric);
  },
  deleteMetric(id) {
    return axios.delete(`${API_URL}/${id}`);
  }
};
