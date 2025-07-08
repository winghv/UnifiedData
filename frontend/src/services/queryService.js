import axios from 'axios';

const API_URL = '/api/query';

export default {
  executeQuery(sql) {
    return axios.post(API_URL, sql, {
      headers: { 'Content-Type': 'text/plain' },
      responseType: 'arraybuffer' // Important for handling Arrow binary format
    });
  }
};
