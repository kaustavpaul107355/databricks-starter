import { useEffect, useRef, useState } from "react";
import { Header } from "@/app/components/Header";
import { TabNavigation } from "@/app/components/TabNavigation";
import { ExecutiveSummary } from "@/app/components/ExecutiveSummary";
import { RevenueAnalytics } from "@/app/components/RevenueAnalytics";
import { Operations } from "@/app/components/Operations";
import { CustomerInsights } from "@/app/components/CustomerInsights";
import { MapView } from "@/app/components/MapView";
import { GovernanceFooter } from "@/app/components/GovernanceFooter";

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

  const handleSpeak = (text: string) => {
    if (!("speechSynthesis" in window)) {
      setAiResponse("Text-to-speech isn't supported in this browser.");
      return;
    }

    window.speechSynthesis.cancel();
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = "en-US";
    utterance.rate = 1;
    window.speechSynthesis.speak(utterance);
  };

  useEffect(() => {
    return () => recognitionRef.current?.stop();
  }, []);

  const renderTabContent = () => {
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
        return <RevenueAnalytics />;
      case "operations":
        return <Operations />;
      case "customers":
        return <CustomerInsights />;
      case "map":
        return <MapView />;
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      <Header />
      <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />
      
      <main className="max-w-7xl mx-auto px-6 py-8">
        {renderTabContent()}
      </main>
      
      <GovernanceFooter />
    </div>
  );
}