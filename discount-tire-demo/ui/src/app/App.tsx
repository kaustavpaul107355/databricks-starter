import { useEffect, useRef, useState, lazy, Suspense } from "react";
import { Header } from "@/app/components/Header";
import { TabNavigation } from "@/app/components/TabNavigation";
import { ExecutiveSummary } from "@/app/components/ExecutiveSummary";
import { GovernanceFooter } from "@/app/components/GovernanceFooter";

// Lazy load heavy components for better initial load performance
const RevenueAnalytics = lazy(() => import("@/app/components/RevenueAnalytics").then(m => ({ default: m.RevenueAnalytics })));
const Operations = lazy(() => import("@/app/components/Operations").then(m => ({ default: m.Operations })));
const CustomerInsights = lazy(() => import("@/app/components/CustomerInsights").then(m => ({ default: m.CustomerInsights })));
const MapView = lazy(() => import("@/app/components/MapView").then(m => ({ default: m.MapView })));

type InputState = "idle" | "listening" | "processing" | "responded";

type GenieResponse = {
  summary?: string;
  table?: {
    columns: string[];
    rows: Array<Array<string | null>>;
  } | null;
  error?: string;
};

export default function App() {
  const [activeTab, setActiveTab] = useState("home");
  const [inputState, setInputState] = useState<InputState>("idle");
  const [aiResponse, setAiResponse] = useState<string | null>(null);
  const [aiTable, setAiTable] = useState<GenieResponse["table"]>(null);
  const [aiQuestion, setAiQuestion] = useState<string | null>(null);
  const [voiceDraft, setVoiceDraft] = useState<string | null>(null);
  const [isTabTransitioning, setIsTabTransitioning] = useState(false);
  const recognitionRef = useRef<SpeechRecognition | null>(null);

  const handleQuerySubmit = async (query: string) => {
    setInputState("processing");
    setAiQuestion(query);
    setAiResponse(null);
    setAiTable(null);
    setVoiceDraft(null);

    try {
      const response = await fetch("/api/genie/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: query }),
      });

      const payload = (await response.json()) as GenieResponse;
      if (!response.ok) {
        throw new Error(payload.error || "Unable to reach Genie.");
      }

      setAiResponse(payload.summary || "No summary returned from Genie.");
      setAiTable(payload.table || null);
      setInputState("responded");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setAiResponse(`Genie request failed: ${message}`);
      setAiTable(null);
      setInputState("responded");
    }
  };

  const handleVoiceInput = () => {
    const SpeechRecognitionImpl =
      window.SpeechRecognition || window.webkitSpeechRecognition;

    if (!SpeechRecognitionImpl) {
      setAiResponse("Voice input isn't supported in this browser.");
      setInputState("responded");
      return;
    }

    const recognition = new SpeechRecognitionImpl();
    recognition.lang = "en-US";
    recognition.interimResults = true;
    recognition.maxAlternatives = 3;

    recognition.onresult = (event) => {
      let transcript = "";
      for (let i = event.resultIndex; i < event.results.length; i += 1) {
        transcript += event.results[i][0].transcript;
      }
      transcript = transcript.trim();
      if (transcript) {
        setVoiceDraft(transcript);
      }
      if (event.results[event.results.length - 1]?.isFinal) {
        setInputState("idle");
        recognition.stop();
      }
    };

    recognition.onerror = () => {
      setInputState("idle");
    };

    recognition.onend = () => {
      setInputState((prev) => (prev === "listening" ? "idle" : prev));
    };

    recognitionRef.current?.stop();
    recognitionRef.current = recognition;
    setInputState("listening");
    recognition.start();
  };

  const pickPreferredVoice = (voices: SpeechSynthesisVoice[]) => {
    const preferredNames = [
      "Samantha",
      "Alex",
      "Google US English",
      "Google UK English Female",
      "Microsoft Aria Online (Natural) - English (United States)",
      "Microsoft Jenny Online (Natural) - English (United States)",
    ];
    const preferred = voices.find((voice) => preferredNames.includes(voice.name));
    if (preferred) {
      return preferred;
    }
    const english = voices.find((voice) => voice.lang.toLowerCase().startsWith("en"));
    return english || voices[0] || null;
  };

  const splitForSpeech = (text: string) =>
    text
      .replace(/\s+/g, " ")
      .split(/(?<=[.!?])\s+/)
      .map((part) => part.trim())
      .filter(Boolean);

  const handleSpeak = (text: string) => {
    if (!("speechSynthesis" in window)) {
      setAiResponse("Text-to-speech isn't supported in this browser.");
      return;
    }

    window.speechSynthesis.cancel();
    const voices = window.speechSynthesis.getVoices();
    const voice = pickPreferredVoice(voices);
    
    // Enhanced text processing: add natural pauses at punctuation
    const processedText = text
      .replace(/([.!?])\s+/g, "$1... ") // Add pause after sentence-ending punctuation
      .replace(/([,;:])\s+/g, "$1. "); // Add slight pause after commas, semicolons, colons
    
    const segments = splitForSpeech(processedText);

    const speakSegment = (index: number) => {
      if (index >= segments.length) {
        return;
      }
      const utterance = new SpeechSynthesisUtterance(segments[index]);
      utterance.lang = "en-US";
      utterance.rate = 0.9; // Slightly slower for more natural cadence
      utterance.pitch = 1.0; // Neutral pitch sounds more natural
      utterance.volume = 0.9; // Slightly softer volume
      if (voice) {
        utterance.voice = voice;
      }
      // Add a small pause between segments for better flow
      utterance.onend = () => {
        setTimeout(() => speakSegment(index + 1), 150);
      };
      window.speechSynthesis.speak(utterance);
    };

    speakSegment(0);
  };

  useEffect(() => {
    return () => recognitionRef.current?.stop();
  }, []);

  useEffect(() => {
    // Trigger fade out/blur when tab changes
    setIsTabTransitioning(true);
    // Keep in transitioning state briefly, then fade in
    const timer = setTimeout(() => setIsTabTransitioning(false), 100);
    return () => clearTimeout(timer);
  }, [activeTab]);

  const renderTabContent = () => {
    // Loading fallback for lazy-loaded components
    const LoadingFallback = () => (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );

    switch (activeTab) {
      case "home":
        return (
          <ExecutiveSummary
            inputState={inputState}
            aiResponse={aiResponse}
            aiTable={aiTable}
            aiQuestion={aiQuestion}
            prefillText={voiceDraft}
            onQuerySubmit={handleQuerySubmit}
            onVoiceInput={handleVoiceInput}
            onSpeak={handleSpeak}
          />
        );
      case "revenue":
        return (
          <Suspense fallback={<LoadingFallback />}>
            <RevenueAnalytics />
          </Suspense>
        );
      case "operations":
        return (
          <Suspense fallback={<LoadingFallback />}>
            <Operations />
          </Suspense>
        );
      case "customers":
        return (
          <Suspense fallback={<LoadingFallback />}>
            <CustomerInsights />
          </Suspense>
        );
      case "map":
        return (
          <Suspense fallback={<LoadingFallback />}>
            <MapView />
          </Suspense>
        );
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen app-shell">
      <Header />
      <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />
      
      <main className="max-w-7xl mx-auto px-6 py-8">
        <div 
          className={`transition-all duration-300 ease-in-out ${
            isTabTransitioning 
              ? 'opacity-0 blur-sm scale-98' 
              : 'opacity-100 blur-0 scale-100'
          }`}
        >
          {renderTabContent()}
        </div>
      </main>
      
      <GovernanceFooter />
    </div>
  );
}