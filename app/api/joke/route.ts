import { NextRequest, NextResponse } from 'next/server';

// Mock joke generation for demonstration
// In a real implementation, this would call the Genkit AI flow
const jokes: Record<string, string[]> = {
  dad: [
    "Why don't scientists trust atoms? Because they make up everything!",
    "I used to hate facial hair, but then it grew on me.",
    "Why did the scarecrow win an award? Because he was outstanding in his field!",
    "I'm reading a book about anti-gravity. It's impossible to put down!",
  ],
  'knock-knock': [
    "Knock knock. Who's there? Lettuce. Lettuce who? Lettuce in, it's cold out here!",
    "Knock knock. Who's there? Banana. Banana who? Knock knock. Who's there? Orange. Orange who? Orange you glad I didn't say banana?",
  ],
  programming: [
    "Why do programmers prefer dark mode? Because light attracts bugs!",
    "A SQL query walks into a bar, walks up to two tables and asks... 'Can I join you?'",
    "Why do Java developers wear glasses? Because they don't C#!",
  ],
  default: [
    "Why don't eggs tell jokes? They'd crack each other up!",
    "What do you call a fake noodle? An impasta!",
    "Why did the coffee file a police report? It got mugged!",
  ],
};

function getRandomJoke(type: string | null): string {
  const jokeType = type?.toLowerCase() || 'default';
  const jokeList = jokes[jokeType] || jokes.default;
  return jokeList[Math.floor(Math.random() * jokeList.length)];
}

// Handle POST requests for both run and stream modes
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const input = body.data;
    const acceptHeader = request.headers.get('accept') || '';
    
    const joke = getRandomJoke(input);
    
    // Check if client wants streaming response
    if (acceptHeader.includes('text/event-stream')) {
      // Streaming response - simulate chunked delivery
      const encoder = new TextEncoder();
      const words = joke.split(' ');
      
      const stream = new ReadableStream({
        async start(controller) {
          // Stream word by word with small delays
          for (let i = 0; i < words.length; i++) {
            const word = (i === 0 ? '' : ' ') + words[i];
            const chunk = `data: ${JSON.stringify({ message: word })}\n\n`;
            controller.enqueue(encoder.encode(chunk));
            // Small delay to simulate streaming
            await new Promise(resolve => setTimeout(resolve, 100));
          }
          // Send the final result
          const resultChunk = `data: ${JSON.stringify({ result: joke })}\n\n`;
          controller.enqueue(encoder.encode(resultChunk));
          controller.close();
        },
      });
      
      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        },
      });
    } else {
      // Non-streaming response
      return NextResponse.json({ result: joke });
    }
  } catch (error) {
    return NextResponse.json(
      { error: { message: 'Failed to generate joke', status: 500 } },
      { status: 500 }
    );
  }
}
