<template>
  <div class="stock-data-viewer">
    <h2>Stock Market Data</h2>
    
    <div class="data-source-selector">
      <el-radio-group v-model="dataSource" @change="loadData">
        <el-radio-button label="json">JSON Data</el-radio-button>
        <el-radio-button label="csv">CSV Data</el-radio-button>
      </el-radio-group>
      
      <el-button 
        type="primary" 
        @click="loadData" 
        :loading="loading"
        style="margin-left: 20px;"
      >
        Refresh Data
      </el-button>
    </div>
    
    <el-table 
      :data="stockData" 
      border 
      stripe 
      style="width: 100%; margin-top: 20px;"
      v-loading="loading"
    >
      <el-table-column prop="stkcode" label="Stock Code" width="120" />
      <el-table-column prop="timestamp" label="Date" width="180">
        <template #default="{ row }">
          {{ formatDate(row.timestamp) }}
        </template>
      </el-table-column>
      <el-table-column prop="close" label="Price" width="120" align="right" />
      <el-table-column prop="macd" label="MACD" width="120" align="right">
        <template #default="{ row }">
          <span :class="{ 'positive': parseFloat(row.macd) > 0, 'negative': parseFloat(row.macd) < 0 }">
            {{ row.macd }}
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="pe" label="P/E" width="120" align="right" />
      <el-table-column prop="volume" label="Volume" align="right">
        <template #default="{ row }">
          {{ formatNumber(row.volume) }}
        </template>
      </el-table-column>
    </el-table>
    
    <div class="data-preview" v-if="rawData">
      <h3>Raw Data Preview</h3>
      <pre>{{ rawDataPreview }}</pre>
    </div>
  </div>
</template>

<script>
export default {
  name: 'StockDataViewer',
  data() {
    return {
      dataSource: 'json',
      stockData: [],
      rawData: null,
      loading: false
    };
  },
  computed: {
    rawDataPreview() {
      if (!this.rawData) return '';
      return JSON.stringify(this.rawData, null, 2);
    }
  },
  mounted() {
    this.loadData();
  },
  methods: {
    async loadData() {
      this.loading = true;
      try {
        const backendUrl = 'http://localhost:8080';
        const response = await fetch(`${backendUrl}/api/stock-data?source=${this.dataSource}`, {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          mode: 'cors',
          credentials: 'omit'
        });
        
        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Failed to fetch stock data: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        this.rawData = data;
        this.stockData = data.status_code === 0 ? data.datas : [];
      } catch (error) {
        console.error('Error loading stock data:', error);
        this.$message.error(`Failed to load stock data: ${error.message}`);
      } finally {
        this.loading = false;
      }
    },
    formatDate(timestamp) {
      if (!timestamp) return '';
      const date = new Date(parseInt(timestamp));
      return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
    },
    formatNumber(num) {
      if (!num) return '0';
      return parseInt(num).toLocaleString();
    }
  }
};
</script>

<style scoped>
.stock-data-viewer {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.data-source-selector {
  margin: 20px 0;
  display: flex;
  align-items: center;
}

.data-preview {
  margin-top: 30px;
  background: #f5f7fa;
  padding: 15px;
  border-radius: 4px;
  max-height: 300px;
  overflow: auto;
}

.data-preview pre {
  margin: 0;
  font-family: monospace;
  font-size: 12px;
}

.positive {
  color: #f56c6c;
  font-weight: bold;
}

.negative {
  color: #67c23a;
  font-weight: bold;
}

h2 {
  color: #303133;
  margin-bottom: 20px;
}

h3 {
  color: #606266;
  margin: 10px 0;
}
</style>
