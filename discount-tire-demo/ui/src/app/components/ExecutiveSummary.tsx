import { AIInteractionPanel } from "@/app/components/AIInteractionPanel";
import { KPIMetrics } from "@/app/components/KPIMetrics";
import { ChartSection } from "@/app/components/ChartSection";
import { FollowUpQuestions } from "@/app/components/FollowUpQuestions";

interface ExecutiveSummaryProps {
  inputState: "idle" | "listening" | "processing" | "responded";
  aiResponse: string | null;
  aiQuestion?: string | null;
  aiTable?: {
    columns: string[];
    rows: Array<Array<string | null>>;
  } | null;
  prefillText?: string | null;
  onQuerySubmit: (query: string) => void;
  onVoiceInput: () => void;
  onSpeak: (text: string) => void;
}

export function ExecutiveSummary({
  inputState,
  aiResponse,
  aiQuestion,
  aiTable,
  prefillText,
  onQuerySubmit,
  onVoiceInput,
  onSpeak,
}: ExecutiveSummaryProps) {
  return (
    <div className="space-y-8">
      <AIInteractionPanel
        inputState={inputState}
        aiQuestion={aiQuestion}
        aiResponse={aiResponse}
        aiTable={aiTable}
        prefillText={prefillText}
        onQuerySubmit={onQuerySubmit}
        onVoiceInput={onVoiceInput}
        onSpeak={onSpeak}
      />
      <FollowUpQuestions onQuestionClick={onQuerySubmit} />

      <KPIMetrics />

      <ChartSection />
    </div>
  );
}
