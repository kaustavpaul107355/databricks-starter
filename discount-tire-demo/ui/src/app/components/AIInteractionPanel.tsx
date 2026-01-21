import { useEffect, useState } from "react";
import { Mic, Send, Volume2, Loader2 } from "lucide-react";

interface AIInteractionPanelProps {
  inputState: "idle" | "listening" | "processing" | "responded";
  aiQuestion?: string | null;
  aiResponse: string | null;
  aiTable?: {
    columns: string[];
    rows: Array<Array<string | null>>;
  } | null;
  prefillText?: string | null;
  onQuerySubmit: (query: string) => void;
  onVoiceInput: () => void;
  onSpeak: (text: string) => void;
}

export function AIInteractionPanel({
  inputState,
  aiQuestion,
  aiResponse,
  aiTable,
  prefillText,
  onQuerySubmit,
  onVoiceInput,
  onSpeak,
}: AIInteractionPanelProps) {
  const [inputValue, setInputValue] = useState("");

  const renderInline = (text: string) => {
    const parts: Array<JSX.Element | string> = [];
    const regex = /(\*\*[^*]+\*\*|\*[^*]+\*)/g;
    let lastIndex = 0;
    let match: RegExpExecArray | null;

    while ((match = regex.exec(text)) !== null) {
      if (match.index > lastIndex) {
        parts.push(text.slice(lastIndex, match.index));
      }
      const token = match[0];
      const content = token.replace(/^\*+|\*+$/g, "");
      if (token.startsWith("**")) {
        parts.push(
          <strong key={`${match.index}-bold`} className="font-semibold text-gray-900">
            {content}
          </strong>
        );
      } else {
        parts.push(
          <em key={`${match.index}-italic`} className="italic text-gray-700">
            {content}
          </em>
        );
      }
      lastIndex = match.index + token.length;
    }
    if (lastIndex < text.length) {
      parts.push(text.slice(lastIndex));
    }
    return parts;
  };

  const renderResponse = (text: string) => {
    const lines = text.split(/\r?\n/);
    const blocks: JSX.Element[] = [];
    let listItems: string[] = [];
    let listType: "bullet" | "number" = "bullet";

    const flushList = () => {
      if (!listItems.length) {
        return;
      }
      const ListTag = listType === "number" ? "ol" : "ul";
      const listClassName =
        listType === "number"
          ? "list-decimal pl-5 space-y-1 text-sm text-gray-700"
          : "list-disc pl-5 space-y-1 text-sm text-gray-700";
      blocks.push(
        <ListTag key={`list-${blocks.length}`} className={listClassName}>
          {listItems.map((item, index) => (
            <li key={`item-${index}`}>{renderInline(item)}</li>
          ))}
        </ListTag>
      );
      listItems = [];
      listType = "bullet";
    };

    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        flushList();
        return;
      }
      const isBullet = /^[-*•]\s+/.test(trimmed);
      const isNumbered = /^\d+[\.\)]\s+/.test(trimmed);
      if (isBullet || isNumbered) {
        if (isNumbered) {
          listType = "number";
        }
        listItems.push(trimmed.replace(/^([-*•]|\d+[\.\)])\s+/, ""));
        return;
      }

      const hasInlineBullets = /\s[-•]\s+/.test(trimmed);
      if (hasInlineBullets) {
        const parts = trimmed.split(/\s[-•]\s+/).filter(Boolean);
        if (parts.length > 1) {
          const intro = parts.shift();
          if (intro) {
            blocks.push(
              <p key={`para-${blocks.length}`} className="text-sm leading-relaxed text-gray-700">
                {renderInline(intro.trim())}
              </p>
            );
          }
          listItems = parts.map((part) => part.trim());
          listType = "bullet";
          flushList();
          return;
        }
      }
      flushList();
      blocks.push(
        <p key={`para-${blocks.length}`} className="text-sm leading-relaxed text-gray-700">
          {renderInline(trimmed)}
        </p>
      );
    });

    flushList();
    return <div className="space-y-2">{blocks}</div>;
  };

  useEffect(() => {
    if (!prefillText) {
      return;
    }
    if (inputState === "listening" || inputValue.trim() === "") {
      setInputValue(prefillText);
    }
  }, [prefillText, inputState, inputValue]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputValue.trim()) {
      onQuerySubmit(inputValue);
      setInputValue("");
    }
  };

  return (
    <div className="space-y-6">
      {/* Input Panel */}
      <div className="glass-panel rounded-2xl p-8">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="flex items-center gap-4">
            {/* Voice Button */}
            <button
              type="button"
              onClick={onVoiceInput}
              disabled={inputState === "listening" || inputState === "processing"}
              className={`relative flex-shrink-0 w-16 h-16 rounded-full flex items-center justify-center transition-all ${
                inputState === "listening"
                  ? "bg-red-500 animate-pulse shadow-lg shadow-red-200"
                  : "bg-blue-600 hover:bg-blue-700 shadow-md hover:shadow-lg"
              } disabled:opacity-50`}
            >
              {inputState === "listening" && (
                <span className="absolute inset-0 rounded-full bg-red-400 opacity-30 animate-ping" />
              )}
              <Mic className="w-7 h-7 text-white" />
            </button>

            {/* Text Input */}
            <div className="flex-1 relative">
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                placeholder="Ask about revenue, tire sales, inventory health, or customer satisfaction..."
                disabled={inputState === "listening" || inputState === "processing"}
                className="w-full px-6 py-4 rounded-xl border-2 border-indigo-400 bg-white/80 backdrop-blur-sm
                  shadow-md shadow-indigo-200/70 
                  focus:border-indigo-500 focus:shadow-lg focus:shadow-indigo-300 focus:scale-[1.01]
                  transition-all duration-300 outline-none 
                  disabled:bg-gray-50 disabled:border-gray-300 disabled:shadow-gray-200/50"
              />
              {inputValue && (
                <button
                  type="submit"
                  className="absolute right-3 top-1/2 -translate-y-1/2 p-2 rounded-lg 
                    bg-gradient-to-br from-indigo-500 to-indigo-600 
                    hover:from-indigo-600 hover:to-indigo-700 
                    shadow-md hover:shadow-lg hover:scale-105
                    text-white transition-all duration-300"
                >
                  <Send className="w-5 h-5" />
                </button>
              )}
            </div>
          </div>

          {/* Status Text */}
          <div className="text-center">
            {inputState === "idle" && (
              <p className="text-sm text-gray-500">
                {prefillText
                  ? "Voice transcript ready — edit if needed, then send."
                  : "Ask naturally — AI translates your question into live analytics"}
              </p>
            )}
            {inputState === "listening" && (
              <div className="flex flex-col items-center gap-2">
                <p className="text-sm text-red-600 font-medium animate-pulse">
                  🎤 Listening...
                </p>
                <div className="flex items-end gap-1">
                  {[0, 1, 2, 3, 4].map((index) => (
                    <span
                      key={`voice-bar-${index}`}
                      className="w-1.5 rounded-full bg-red-400"
                      style={{
                        height: `${10 + index * 4}px`,
                        animation: "pulse 1s ease-in-out infinite",
                        animationDelay: `${index * 0.12}s`,
                      }}
                    />
                  ))}
                </div>
              </div>
            )}
            {inputState === "processing" && (
              <div className="flex flex-col items-center gap-2 text-blue-600">
                <div className="flex items-center justify-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <p className="text-sm font-medium">AI is analyzing your request...</p>
                </div>
                <div className="h-2 w-56 overflow-hidden rounded-full bg-blue-100">
                  <div className="h-full w-1/2 bg-blue-500 animate-pulse" />
                </div>
              </div>
            )}
          </div>
        </form>
      </div>

      {/* AI Response Summary */}
      {aiResponse && (
        <div className="glass-panel rounded-2xl p-8 bg-gradient-to-br from-blue-50/70 to-indigo-50/70">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900 mb-1">AI Executive Summary</h3>
              <p className="text-xs text-gray-500">Generated just now</p>
            </div>
            <button
              onClick={() => aiResponse && onSpeak(aiResponse)}
              className="p-2 rounded-lg hover:bg-white/50 transition-colors"
            >
              <Volume2 className="w-5 h-5 text-blue-600" />
            </button>
          </div>

          {aiQuestion && (
            <div className="mb-4 rounded-lg border border-blue-100 bg-white/70 px-4 py-3">
              <p className="text-xs uppercase tracking-wide text-blue-700 mb-1">Question</p>
              <p className="text-sm text-gray-700">{aiQuestion}</p>
            </div>
          )}
          
          {renderResponse(aiResponse)}
          {aiTable && aiTable.columns.length > 0 && aiTable.rows.length > 0 && (
            <div className="mt-6 rounded-xl bg-white/80 border border-blue-100 p-4">
              <p className="text-xs uppercase tracking-wide text-blue-700 mb-2">Result Preview</p>
              <div className="overflow-x-auto">
                <table className="min-w-full text-xs text-gray-700">
                  <thead className="border-b border-blue-100">
                    <tr>
                      {aiTable.columns.map((column) => (
                        <th key={column} className="text-left font-semibold py-2 pr-4">
                          {column}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {aiTable.rows.map((row, rowIndex) => (
                      <tr key={`${rowIndex}`} className="border-b border-blue-50 last:border-0">
                        {row.map((value, valueIndex) => (
                          <td key={`${rowIndex}-${valueIndex}`} className="py-2 pr-4 whitespace-nowrap">
                            {value ?? "—"}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
