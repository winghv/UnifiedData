<template>
  <div>
    <el-card>
      <template #header>
        <span>Ad-Hoc SQL Query</span>
      </template>

      <el-input
        v-model="sqlQuery"
        :rows="8"
        type="textarea"
        placeholder="Enter your SQL query here"
      />

      <div class="button-container">
        <el-button type="primary" @click="executeQuery" :loading="loading">Execute</el-button>
      </div>

      <el-divider v-if="results.length > 0 || columns.length > 0"></el-divider>

      <div v-if="loading" class="loading-container">
        <p>Loading results...</p>
      </div>

      <div v-if="!loading && results.length > 0">
        <h3>Results</h3>
        <el-table :data="results" stripe border height="400">
          <el-table-column
            v-for="column in columns"
            :key="column.name"
            :prop="column.name"
            :label="column.name"
            show-overflow-tooltip
          >
          </el-table-column>
        </el-table>
      </div>

       <div v-if="!loading && queryExecuted && results.length === 0" class="no-results-container">
        <p>Query executed successfully, but returned no results.</p>
      </div>

    </el-card>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { ElMessage } from 'element-plus';
import queryService from '../services/queryService';
import { tableFromIPC } from 'apache-arrow';

const sqlQuery = ref("SELECT ticker, date, volume, price FROM stock_quote WHERE ticker = 'AAPL' AND date = 1672531200000");
const loading = ref(false);
const results = ref([]);
const columns = ref([]);
const queryExecuted = ref(false);

const executeQuery = async () => {
  if (!sqlQuery.value.trim()) {
    ElMessage.warning('SQL query cannot be empty.');
    return;
  }

  loading.value = true;
  queryExecuted.value = false;
  results.value = [];
  columns.value = [];

  try {
    const response = await queryService.executeQuery(sqlQuery.value);
    const arrowTable = tableFromIPC(response.data);

    const schema = arrowTable.schema;
    columns.value = schema.fields.map(field => ({ name: field.name }));

    const data = [];
    for (let i = 0; i < arrowTable.numRows; i++) {
      const row = {};
      for (const field of schema.fields) {
        const value = arrowTable.getChild(field.name)?.get(i);
        row[field.name] = value;
      }
      data.push(row);
    }
    results.value = data;
    ElMessage.success('Query executed successfully.');

  } catch (error) {
    const errorMessage = error.response?.data ? new TextDecoder().decode(error.response.data) : 'Failed to execute query.';
    ElMessage.error(errorMessage);
  } finally {
    loading.value = false;
    queryExecuted.value = true;
  }
};
</script>

<style scoped>
.button-container {
  margin-top: 20px;
  text-align: right;
}
.loading-container, .no-results-container {
  margin-top: 20px;
  text-align: center;
  color: #909399;
}
</style>
