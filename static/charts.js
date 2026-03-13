import { fmtNumber } from "./formatters.js";

export function drawChart(canvas, points, color, label) {
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#0f1530";
  ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(148, 163, 184, 0.18)";
  for (let index = 0; index < 4; index += 1) {
    const y = 20 + index * 40;
    ctx.beginPath();
    ctx.moveTo(40, y);
    ctx.lineTo(width - 12, y);
    ctx.stroke();
  }
  if (!points.length) {
    ctx.fillStyle = "#94a3b8";
    ctx.font = "12px sans-serif";
    ctx.fillText("暂无数据", 40, 90);
    return;
  }
  const values = points.map((point) => Number(point.value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = 40 + (index / Math.max(points.length - 1, 1)) * (width - 56);
    const y = height - 24 - ((Number(point.value) - min) / range) * (height - 48);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#e2e8f0";
  ctx.font = "12px sans-serif";
  ctx.fillText(`${label}: ${fmtNumber(values[values.length - 1], 3)}`, 40, 16);
}
