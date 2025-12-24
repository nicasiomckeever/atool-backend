// Edge Function: Route queue operations to worker Supabase projects
// This function distributes load across multiple free-tier Supabase projects

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

// Worker Supabase project configurations
const WORKERS = [
  {
    id: 'worker-1',
    url: Deno.env.get('WORKER_1_URL') || '',
    key: Deno.env.get('WORKER_1_ANON_KEY') || '',
    priority: 1,
  },
  {
    id: 'worker-2',
    url: Deno.env.get('WORKER_2_URL') || '',
    key: Deno.env.get('WORKER_2_ANON_KEY') || '',
    priority: 2,
  },
  {
    id: 'worker-3',
    url: Deno.env.get('WORKER_3_URL') || '',
    key: Deno.env.get('WORKER_3_ANON_KEY') || '',
    priority: 3,
  },
]

// Health check state (in-memory, resets on cold start)
const workerHealth: Record<string, { healthy: boolean; lastCheck: number; failCount: number }> = {}

// Initialize health state
WORKERS.forEach(worker => {
  workerHealth[worker.id] = { healthy: true, lastCheck: Date.now(), failCount: 0 }
})

// Round-robin counter for load balancing
let roundRobinCounter = 0

/**
 * Select a healthy worker using round-robin with failover
 */
function selectWorker(): typeof WORKERS[0] | null {
  const healthyWorkers = WORKERS.filter(w => {
    const health = workerHealth[w.id]
    // Consider unhealthy if failed more than 3 times in last 5 minutes
    if (health.failCount > 3 && Date.now() - health.lastCheck < 300000) {
      return false
    }
    return true
  })

  if (healthyWorkers.length === 0) {
    console.error('No healthy workers available!')
    return null
  }

  // Round-robin selection
  const worker = healthyWorkers[roundRobinCounter % healthyWorkers.length]
  roundRobinCounter++
  
  return worker
}

/**
 * Mark worker as failed and update health state
 */
function markWorkerFailed(workerId: string) {
  const health = workerHealth[workerId]
  if (health) {
    health.failCount++
    health.lastCheck = Date.now()
    health.healthy = false
    console.warn(`Worker ${workerId} marked as unhealthy. Fail count: ${health.failCount}`)
  }
}

/**
 * Mark worker as healthy
 */
function markWorkerHealthy(workerId: string) {
  const health = workerHealth[workerId]
  if (health) {
    health.failCount = 0
    health.lastCheck = Date.now()
    health.healthy = true
  }
}

/**
 * Execute operation with automatic failover
 */
async function executeWithFailover(
  operation: (worker: typeof WORKERS[0]) => Promise<any>,
  maxRetries = 3
): Promise<{ data: any; error: any; worker: string | null }> {
  let lastError = null
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const worker = selectWorker()
    
    if (!worker) {
      return {
        data: null,
        error: 'No healthy workers available',
        worker: null
      }
    }

    try {
      console.log(`Attempt ${attempt + 1}: Using worker ${worker.id}`)
      const result = await operation(worker)
      
      // Success - mark worker as healthy
      markWorkerHealthy(worker.id)
      
      return {
        data: result,
        error: null,
        worker: worker.id
      }
    } catch (error) {
      console.error(`Worker ${worker.id} failed:`, error)
      lastError = error
      markWorkerFailed(worker.id)
      
      // Continue to next worker
      continue
    }
  }

  return {
    data: null,
    error: lastError || 'All workers failed',
    worker: null
  }
}

/**
 * Main request handler
 */
serve(async (req) => {
  // CORS headers
  if (req.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
      },
    })
  }

  try {
    // Verify authentication (either apikey or Authorization header)
    const authHeader = req.headers.get('authorization')
    const apiKey = req.headers.get('apikey')
    
    if (!authHeader && !apiKey) {
      return new Response(
        JSON.stringify({ error: 'Missing authentication' }),
        { 
          status: 401,
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        }
      )
    }

    const { operation, table, data, filters } = await req.json()

    // Validate request
    if (!operation || !table) {
      return new Response(
        JSON.stringify({ error: 'Missing operation or table parameter' }),
        { 
          status: 400,
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        }
      )
    }

    console.log(`Routing ${operation} on table ${table}`)

    // Execute operation with failover
    const result = await executeWithFailover(async (worker) => {
      const supabase = createClient(worker.url, worker.key)
      
      let query = supabase.from(table)

      switch (operation) {
        case 'insert':
          const insertResult = await query.insert(data)
          if (insertResult.error) throw insertResult.error
          return insertResult
        
        case 'update':
          if (!filters) throw new Error('Update requires filters')
          query = applyFilters(query, filters)
          const updateResult = await query.update(data)
          if (updateResult.error) throw updateResult.error
          return updateResult
        
        case 'delete':
          if (!filters) throw new Error('Delete requires filters')
          query = applyFilters(query, filters)
          const deleteResult = await query.delete()
          if (deleteResult.error) throw deleteResult.error
          return deleteResult
        
        case 'select':
          if (filters) {
            query = applyFilters(query, filters)
          }
          const selectResult = await query.select()
          if (selectResult.error) throw selectResult.error
          return selectResult
        
        default:
          throw new Error(`Unsupported operation: ${operation}`)
      }
    })

    if (result.error) {
      throw result.error
    }

    return new Response(
      JSON.stringify({
        success: true,
        data: result.data,
        worker: result.worker,
        timestamp: new Date().toISOString()
      }),
      {
        status: 200,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      }
    )

  } catch (error) {
    console.error('Edge function error:', error)
    
    return new Response(
      JSON.stringify({
        success: false,
        error: error.message || 'Internal server error',
        timestamp: new Date().toISOString()
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      }
    )
  }
})

/**
 * Apply filters to Supabase query
 */
function applyFilters(query: any, filters: Record<string, any>) {
  for (const [key, value] of Object.entries(filters)) {
    if (key === 'eq') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.eq(field, val)
      }
    } else if (key === 'in') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.in(field, val)
      }
    } else if (key === 'gt') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.gt(field, val)
      }
    } else if (key === 'lt') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.lt(field, val)
      }
    } else if (key === 'gte') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.gte(field, val)
      }
    } else if (key === 'lte') {
      for (const [field, val] of Object.entries(value as Record<string, any>)) {
        query = query.lte(field, val)
      }
    } else if (key === 'order') {
      const { column, ascending = true } = value as { column: string; ascending?: boolean }
      query = query.order(column, { ascending })
    } else if (key === 'limit') {
      query = query.limit(value as number)
    }
  }
  return query
}

/* To deploy this edge function:

1. Install Supabase CLI:
   npm install -g supabase

2. Login to Supabase:
   supabase login

3. Link your project:
   supabase link --project-ref your-project-ref

4. Set environment variables:
   supabase secrets set WORKER_1_URL=https://xxx.supabase.co
   supabase secrets set WORKER_1_ANON_KEY=your-key
   supabase secrets set WORKER_2_URL=https://yyy.supabase.co
   supabase secrets set WORKER_2_ANON_KEY=your-key
   supabase secrets set WORKER_3_URL=https://zzz.supabase.co
   supabase secrets set WORKER_3_ANON_KEY=your-key

5. Deploy:
   supabase functions deploy route-queue

6. Your edge function URL will be:
   https://your-project-ref.supabase.co/functions/v1/route-queue
*/
