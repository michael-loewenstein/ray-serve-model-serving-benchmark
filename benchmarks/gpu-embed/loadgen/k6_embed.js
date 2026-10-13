import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { SharedArray } from 'k6/data';

export const successfulEmbeddings = new Counter('successful_embeddings');
export const failedEmbeddings = new Counter('failed_embeddings');
export const serverModelLatency = new Trend('server_model_latency_ms');
export const serverTotalLatency = new Trend('server_total_latency_ms');
export const networkRoundtripLatency = new Trend('network_roundtrip_latency_ms');

const payloads = new SharedArray('queries', function () {
  const fileContent = open('../fixtures/queries.jsonl');
  return fileContent
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
});

const TARGET_HOST = __ENV.TARGET_HOST || 'http://localhost:8000';
const EMBED_PATH = __ENV.EMBED_PATH || '/embed';
const MAX_RPS = parseInt(__ENV.MAX_RPS || '50', 10);
const CONCURRENCY = parseInt(__ENV.CONCURRENCY || '0', 10);
const STAGE_DURATION = __ENV.STAGE_DURATION || '1m';
const TEST_PROFILE = __ENV.TEST_PROFILE || 'steady'; // 'steady', 'concurrency', 'sweep'

function getScenarios() {
  if (TEST_PROFILE === 'concurrency' || CONCURRENCY > 0) {
    const vus = CONCURRENCY > 0 ? CONCURRENCY : 16;
    // Closed-loop concurrency scaling: exactly 'vus' concurrent clients in flight.
    // Clients wait for response before issuing next request; request rate naturally
    // throttles as latency increases (no queue blowout, unlike open-loop arrival rate).
    return {
      closed_loop_concurrency: {
        executor: 'constant-vus',
        vus: vus,
        duration: STAGE_DURATION,
      },
    };
  } else if (TEST_PROFILE === 'sweep') {
    return {
      throughput_sweep: {
        executor: 'ramping-arrival-rate',
        startRate: 50,
        timeUnit: '1s',
        preAllocatedVUs: 50,
        maxVUs: 500,
        stages: [
          { target: 50, duration: STAGE_DURATION },
          { target: 100, duration: STAGE_DURATION },
          { target: 200, duration: STAGE_DURATION },
          { target: 300, duration: STAGE_DURATION },
        ],
      },
    };
  } else {
    // Default: 'steady'
    return {
      steady_arrival: {
        executor: 'constant-arrival-rate',
        rate: MAX_RPS,
        timeUnit: '1s',
        duration: STAGE_DURATION,
        preAllocatedVUs: Math.max(20, Math.floor(MAX_RPS * 0.5)),
        maxVUs: Math.max(100, MAX_RPS * 2),
      },
    };
  }
}

export const options = {
  scenarios: getScenarios(),
  thresholds: {
    http_req_failed: ['rate<0.001'],
    http_req_duration: ['p(95)<25'], // Latency budget: p95 < 25 ms
  },
};

export default function () {
  const index = Math.floor(Math.random() * payloads.length);
  const payload = JSON.stringify(payloads[index]);

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: '10s',
  };

  const res = http.post(`${TARGET_HOST}${EMBED_PATH}`, payload, params);

  let isSuccess = false;
  if (res.status === 200) {
    try {
      const body = JSON.parse(res.body);
      if (Array.isArray(body.embedding) && body.embedding.length === 384) {
        isSuccess = true;
        successfulEmbeddings.add(1);

        if (typeof body.model_latency_ms === 'number') {
          serverModelLatency.add(body.model_latency_ms);
        }
        if (typeof body.total_latency_ms === 'number') {
          serverTotalLatency.add(body.total_latency_ms);
          const transit = res.timings.duration - body.total_latency_ms;
          if (transit > 0) {
            networkRoundtripLatency.add(transit);
          }
        }
      }
    } catch (e) {
      isSuccess = false;
    }
  }

  if (!isSuccess) {
    failedEmbeddings.add(1);
  }

  check(res, {
    'status is 200': (r) => r.status === 200,
    'embedding dimension is 384': () => isSuccess,
  });
}
