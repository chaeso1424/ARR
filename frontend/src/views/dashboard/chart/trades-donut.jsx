// src/views/dashboard/chart/trades-donut.js
const buildTradesDonut = ({ newCount, dcaCount }) => ({
  height: 220,
  type: 'donut',
  options: {
    labels: ['New Entries', 'DCA Adds'],
    legend: { position: 'bottom' },
    dataLabels: { enabled: false },
    plotOptions: { 
      pie: { donut: { labels: { show: false } }, expandOnClick: false } 
    },
    colors: ['#2ed8b6', '#4099ff'],
    stroke: { width: 0 },
    tooltip: { y: { formatter: (v) => `${v} trades` } }
  },
  series: [Number(newCount || 0), Number(dcaCount || 0)]
});
export default buildTradesDonut;

