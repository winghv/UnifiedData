import axios from 'axios';

const API_URL = '/api/tables';

export default {
  getTables() {
    return axios.get(API_URL);
  },
  getTable(id) {
    return axios.get(`${API_URL}/${id}`);
  },
  createTable(table) {
    return axios.post(API_URL, table);
  },
  updateTable(id, table) {
    return axios.put(`${API_URL}/${id}`, table);
  },
  deleteTable(id) {
    return axios.delete(`${API_URL}/${id}`);
  }
};
