export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    

    const url = new URL(request.url);
    // e.g. /releases/tag/v1.0/data.pmtiles → proxy to GitHub
    const githubUrl = `https://github.com/${env.GITHUB_REPO}${url.pathname}`;

    // Forward the Range header from the client
    const headers = new Headers();
    if (request.headers.has("Range")) {
      headers.set("Range", request.headers.get("Range")!);
    }

    const response = await fetch(githubUrl, { method: "GET", headers, redirect: "follow" });

    // Clone the response so we can modify headers
    const newHeaders = new Headers(response.headers);
    // Add CORS headers
    for (const [k, v] of Object.entries(corsHeaders())) {
      newHeaders.set(k, v);
    }
    // Expose Range-related headers to the browser
    newHeaders.append("Access-Control-Expose-Headers", "Content-Range, Content-Length, Accept-Ranges, ETag");

	// Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: newHeaders });
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: newHeaders,
    });
  },
};

function corsHeaders(): Record<string, string> {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Range, Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

interface Env {
  GITHUB_REPO: string;
}
