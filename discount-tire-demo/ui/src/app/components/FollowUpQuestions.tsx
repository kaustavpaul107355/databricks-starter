import { MessageCircle } from "lucide-react";

interface FollowUpQuestionsProps {
  onQuestionClick: (question: string) => void;
}

const questions = [
  "What is the distribution of sales by product category?",
  "What is the average customer satisfaction score for sales?",
  "What is the monthly distribution of total revenue over time?",
  "Show revenue by month",
];

const questionColors = [
  { border: "border-blue-400", shadow: "shadow-blue-200/70", hover: "hover:shadow-blue-300" },
  { border: "border-green-400", shadow: "shadow-green-200/70", hover: "hover:shadow-green-300" },
  { border: "border-purple-400", shadow: "shadow-purple-200/70", hover: "hover:shadow-purple-300" },
  { border: "border-pink-400", shadow: "shadow-pink-200/70", hover: "hover:shadow-pink-300" },
];

export function FollowUpQuestions({ onQuestionClick }: FollowUpQuestionsProps) {
  return (
    <div className="glass-panel rounded-xl p-6">
      <div className="flex items-center gap-2 mb-4">
        <MessageCircle className="w-5 h-5 text-blue-600" />
        <h3 className="text-base font-semibold text-gray-900">Suggested Questions</h3>
      </div>
      
      <div className="flex flex-wrap gap-3">
        {questions.map((question, index) => {
          const colors = questionColors[index % questionColors.length];
          return (
            <button
              key={index}
              onClick={() => onQuestionClick(question)}
              className={`
                px-4 py-2.5 bg-white/80 backdrop-blur-sm
                border-2 ${colors.border}
                shadow-md ${colors.shadow} ${colors.hover}
                rounded-full text-sm font-medium text-gray-700 
                hover:text-gray-900 hover:scale-105
                transition-all duration-300
              `}
            >
              {question}
            </button>
          );
        })}
      </div>
    </div>
  );
}
