'use client';

import { useEffect, useRef, useState } from 'react';

const __flowStreamDelimiter = '\n\n';

interface RequestData<T> {
  url: string;
  headers?: Record<string, string>;
  input?: T;
}

async function runFlow<O = string>(req: RequestData<unknown>): Promise<O> {
  const response = await fetch(req.url, {
    method: 'POST',
    body: JSON.stringify({
      data: req.input,
    }),
    headers: {
      'Content-Type': 'application/json',
      ...req.headers,
    },
  });
  if (response.status !== 200) {
    throw new Error(
      `Server returned: ${response.status}: ${await response.text()}`
    );
  }
  const wrappedResult = (await response.json()) as
    | { result: O }
    | { error: unknown };
  if ('error' in wrappedResult) {
    if (typeof wrappedResult.error === 'string') {
      throw new Error(wrappedResult.error);
    }
    throw new Error(JSON.stringify(wrappedResult.error));
  }
  return wrappedResult.result;
}

function streamFlow<O = string, S = string>(
  req: RequestData<unknown>
): {
  readonly output: Promise<O>;
  readonly stream: AsyncIterable<S>;
} {
  let resolveOutput: (value: O) => void;
  let rejectOutput: (reason?: unknown) => void;
  const outputPromise = new Promise<O>((resolve, reject) => {
    resolveOutput = resolve;
    rejectOutput = reject;
  });

  const streamGenerator = async function* (): AsyncGenerator<S> {
    const response = await fetch(req.url, {
      method: 'POST',
      body: JSON.stringify({
        data: req.input,
      }),
      headers: {
        Accept: 'text/event-stream',
        'Content-Type': 'application/json',
        ...req.headers,
      },
    });
    
    if (response.status !== 200) {
      const errMsg = `Server returned: ${response.status}: ${await response.text()}`;
      rejectOutput(new Error(errMsg));
      throw new Error(errMsg);
    }
    
    if (!response.body) {
      const errMsg = 'Response body is empty';
      rejectOutput(new Error(errMsg));
      throw new Error(errMsg);
    }
    
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    
    while (true) {
      const result = await reader.read();
      const decodedValue = decoder.decode(result.value);
      if (decodedValue) {
        buffer += decodedValue;
      }
      while (buffer.includes(__flowStreamDelimiter)) {
        const chunk = JSON.parse(
          buffer
            .substring(0, buffer.indexOf(__flowStreamDelimiter))
            .substring('data: '.length)
        );
        if (chunk.hasOwnProperty('message')) {
          yield chunk.message as S;
        } else if (chunk.hasOwnProperty('result')) {
          resolveOutput(chunk.result);
          return;
        } else if (chunk.hasOwnProperty('error')) {
          const err = new Error(
            `${chunk.error.status}: ${chunk.error.message}\n${chunk.error.details}`
          );
          rejectOutput(err);
          throw err;
        } else {
          const err = new Error('unknown chunk format: ' + JSON.stringify(chunk));
          rejectOutput(err);
          throw err;
        }
        buffer = buffer.substring(
          buffer.indexOf(__flowStreamDelimiter) + __flowStreamDelimiter.length
        );
      }
      if (result.done) break;
    }
  };

  return {
    output: outputPromise,
    stream: streamGenerator(),
  };
}

async function run(type: string, setResponse: (response: string) => void) {
  setResponse('...');
  try {
    const resp = await runFlow<string>({
      url: '/api/joke',
      input: type === '' ? null : type,
    });
    setResponse(resp);
  } catch (error) {
    setResponse(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

async function stream(type: string, setResponse: (response: string) => void) {
  let accum = '';
  setResponse('...');
  try {
    const { stream, output } = streamFlow<string, string>({
      url: '/api/joke',
      input: type === '' ? null : type,
    });
    for await (const chunk of stream) {
      accum = accum + chunk;
      setResponse(accum);
    }
    setResponse(await output);
  } catch (error) {
    setResponse(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

export default function Home() {
  const inputRef = useRef<HTMLInputElement>(null);
  const [response, setResponse] = useState<string>('Pick a joke type');
  
  function focus() {
    if (inputRef.current) {
      inputRef.current.focus();
    }
  }
  
  useEffect(focus, []);
  
  return (
    <main className="min-h-screen p-8">
      <h1 className="text-2xl font-bold mb-6">Genkit Joke Generator</h1>
      <div className="flex gap-3 mb-6">
        <input
          type="text"
          alt="Joke type"
          placeholder="Joke type (e.g., dad, knock-knock)"
          ref={inputRef}
          className="flex-1 max-w-xs px-4 py-2 border border-border rounded-md bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
        />
        <button
          onClick={() => {
            run(inputRef.current!.value, setResponse);
            focus();
          }}
          className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
        >
          Run
        </button>
        <button
          onClick={() => {
            stream(inputRef.current!.value, setResponse);
            focus();
          }}
          className="px-4 py-2 bg-secondary text-secondary-foreground rounded-md hover:bg-secondary/90 transition-colors"
        >
          Stream
        </button>
      </div>
      <div className="p-4 bg-muted rounded-md min-h-[100px] whitespace-pre-wrap">
        {response}
      </div>
    </main>
  );
}
