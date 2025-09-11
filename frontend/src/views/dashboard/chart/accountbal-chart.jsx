// dashboard/chart/analytics-unique-visitor-chart.js
const buildAccountBalanceChart = ({ labels, values }) => ({
  height: 279,
  type: 'line',
  options: {
    chart: { toolbar: { show: false } },
    dataLabels: { enabled: false },
    stroke: { width: 2, curve: 'smooth' },
    legend: { show: false },
    xaxis: {
      type: 'datetime',
      categories: labels, // 'YYYY-MM-DD'
      axisBorder: { show: false },
      labels: { style: { color: '#ccc' } }
    },
    yaxis: {
      labels: { style: { color: '#ccc' } },
      min: (min) => min * 0.999,
      max: (max) => max * 1.001
    },
    colors: ['#4099ff'],
    fill: {
      type: 'gradient',
      gradient: {
        shade: 'light',
        gradientToColors: ['#73b4ff'],
        shadeIntensity: 0.5,
        type: 'horizontal',
        opacityFrom: 1,
        opacityTo: 1,
        stops: [0, 100]
      }
    },
    markers: { size: 4, colors: ['#4099ff'], opacity: 0.9, strokeWidth: 2, hover: { size: 6 } },
    grid: { borderColor: '#cccccc3b' },
    tooltip: { y: { formatter: v => `${(v ?? 0).toFixed(2)} USDT` } }
  },
  series: [{ name: 'Account Balance', data: values }]
});
export default buildAccountBalanceChart;
