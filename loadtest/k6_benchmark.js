import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { SharedArray } from 'k6/data';

// Custom metric definitions for goodput and latency decomposition
export const successfulRequests = new Counter('successful_requests');
export const failedRequests = new Counter('failed_requests');
export const serverModelLatency = new Trend('server_model_latency_ms');
export const serverTotalLatency = new Trend('server_total_latency_ms');
export const networkRoundtripLatency = new Trend('network_roundtrip_latency_ms');

// Load synthetic test payloads once per VU instance
const payloads = new SharedArray('test_payloads', function () {
  const fileContent = open('./fixtures/sample_payloads.jsonl');
  return fileContent
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
});

const TARGET_HOST = __ENV.TARGET_HOST || 'http://localhost:8000';
const INFER_PATH = __ENV.INFER_PATH || '/infer';
const RATE_RPS = parseInt(__ENV.RATE_RPS || __ENV.MAX_RPS || '50', 10);
const STAGE_DURATION = __ENV.STAGE_DURATION || '30s';

export const options = {
  scenarios: {
    constant_arrival_rate: {
      executor: 'constant-arrival-rate',
      rate: RATE_RPS,
      timeUnit: '1s',
      duration: STAGE_DURATION,
      preAllocatedVUs: Math.max(20, Math.floor(RATE_RPS * 0.5)),
      maxVUs: Math.max(100, RATE_RPS * 2),
    },
  },
  thresholds: {
    successful_requests: ['count>0'],
    http_req_failed: ['rate<0.001'], // Target: < 0.1% failure
    http_req_duration: ['p(95)<15', 'p(99)<30'], // Latency targets in ms
  },
};

export default function () {
  // Deterministic circular selection across the 1,000 pre-loaded payloads
  const index = (__VU * 10000 + __ITER) % payloads.length;
  const payload = JSON.stringify(payloads[index]);

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Connection': 'keep-alive',
    },
    timeout: '5s',
  };

  const url = `${TARGET_HOST}${INFER_PATH}`;
  const res = http.post(url, payload, params);

  const isOk = check(res, {
    'status is 200': (r) => r.status === 200,
    'has prediction': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body && typeof body.prediction === 'number';
      } catch (e) {
        return false;
      }
    },
  });

  if (isOk) {
    successfulRequests.add(1);
    try {
      const body = JSON.parse(res.body);
      if (body.model_latency_ms !== undefined) {
        serverModelLatency.add(body.model_latency_ms);
      }
      if (body.total_latency_ms !== undefined) {
        serverTotalLatency.add(body.total_latency_ms);
        // Estimated transit gap: client duration minus server compute duration
        const transitMs = Math.max(0.0, res.timings.duration - body.total_latency_ms);
        networkRoundtripLatency.add(transitMs);
      }
    } catch (e) {
      // Ignored if JSON parsing fails on response
    }
  } else {
    failedRequests.add(1);
  }
}
