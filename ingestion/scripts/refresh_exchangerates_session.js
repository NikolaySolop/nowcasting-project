#!/usr/bin/env node

const { chromium } = require("playwright");

const HISTORY_URL = "https://www.exchangerates.org.uk/commodities/URALS-USD-history.html";

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function parseArgs(argv) {
  const args = {
    headless: false,
    waitMs: 15000,
    from: "2026-05-01",
    to: "2026-05-02",
    per: "2",
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--headless") {
      args.headless = true;
    } else if (arg === "--wait-ms") {
      args.waitMs = Number(argv[++index]);
    } else if (arg === "--from") {
      args.from = argv[++index];
    } else if (arg === "--to") {
      args.to = argv[++index];
    } else if (arg === "--per") {
      args.per = argv[++index];
    }
  }
  return args;
}

async function acceptConsentIfPresent(page) {
  const labels = [
    "Accept",
    "Accept all",
    "I agree",
    "Agree",
    "Consent",
  ];
  for (const label of labels) {
    const button = page.getByRole("button", { name: new RegExp(label, "i") }).first();
    try {
      if (await button.isVisible({ timeout: 1500 })) {
        await button.click({ timeout: 3000 });
        return;
      }
    } catch {
      // Continue probing common consent labels.
    }
  }
}

async function findWorkingNonce(page, args) {
  return page.evaluate(async ({ historyUrl, from, to, per }) => {
    const html = document.documentElement.innerHTML;
    const candidates = new Set();
    const patterns = [
      /(?:nonce|ajaxNonce|ajax_nonce|xAjaxNonce|x_ajax_nonce)["'\s:=]+([a-f0-9]{16,64})/gi,
      /\b[a-f0-9]{32}\b/gi,
    ];
    for (const pattern of patterns) {
      for (const match of html.matchAll(pattern)) {
        candidates.add(match[1] || match[0]);
      }
    }

    const attempts = [];
    for (const nonce of candidates) {
      const form = new FormData();
      form.set("ajax", "hist");
      form.set("from", from);
      form.set("to", to);
      form.set("page", "1");
      form.set("per", per);
      form.set("nonce", nonce);

      const response = await fetch(historyUrl, {
        method: "POST",
        credentials: "include",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
          "X-Ajax-Nonce": nonce,
        },
        body: form,
      });
      const contentType = response.headers.get("content-type") || "";
      const text = await response.text();
      attempts.push({
        nonce,
        status: response.status,
        contentType,
        textStart: text.slice(0, 120),
      });
      if (response.ok && contentType.toLowerCase().includes("json")) {
        try {
          JSON.parse(text);
          return { nonce, attempts };
        } catch {
          // Keep looking.
        }
      }
    }
    return { nonce: null, attempts };
  }, {
    historyUrl: HISTORY_URL,
    from: args.from,
    to: args.to,
    per: args.per,
  });
}

async function main() {
  const args = parseArgs(process.argv);
  const browser = await chromium.launch({ headless: args.headless });
  const context = await browser.newContext({
    locale: "en-GB",
    timezoneId: "Europe/London",
    userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15",
  });
  const page = await context.newPage();

  await page.goto(HISTORY_URL, { waitUntil: "domcontentloaded", timeout: 120000 });
  await acceptConsentIfPresent(page);
  await page.waitForTimeout(args.waitMs);

  const result = await findWorkingNonce(page, args);
  const cookies = await context.cookies("https://www.exchangerates.org.uk");
  await browser.close();

  const cookieHeader = cookies.map((cookie) => `${cookie.name}=${cookie.value}`).join("; ");
  if (!result.nonce) {
    console.error("No working ExchangeRates AJAX nonce found.");
    console.error("Attempts:");
    for (const attempt of result.attempts.slice(0, 10)) {
      console.error(`- status=${attempt.status} content_type=${attempt.contentType} nonce=${attempt.nonce}`);
    }
    process.exit(2);
  }
  if (!cookieHeader) {
    console.error("No cookies were captured for exchangerates.org.uk.");
    process.exit(3);
  }

  console.log(`export EXCHANGERATES_AJAX_NONCE=${shellQuote(result.nonce)}`);
  console.log(`export EXCHANGERATES_COOKIE=${shellQuote(cookieHeader)}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
