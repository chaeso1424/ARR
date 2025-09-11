// src/views/dashboard/chart/profit-chart.jsx
const buildProfitDonut = ({ wins = 0, losses = 0 }) => ({
  height: 220,
  type: 'donut',
  options: {
    labels: ['Winning', 'Losing'],
    legend: {
      position: 'bottom',
      labels: { colors: '#ffffff' }   // 👈 흰색으로 강제 지정
    },
    dataLabels: { enabled: false },
    plotOptions: {
      pie: {
        donut: { labels: { show: false } },
        expandOnClick: false
      }
    },
    colors: ['#2ed8b6', '#4099ff'],
    stroke: { width: 0 },
    tooltip: { y: { formatter: (v) => `${v} trades` } }
  },
  series: [Number(wins || 0), Number(losses || 0)]
});

export default buildProfitDonut;
