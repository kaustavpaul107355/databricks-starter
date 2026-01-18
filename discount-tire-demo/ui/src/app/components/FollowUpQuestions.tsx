import { MessageCircle } from "lucide-react";

interface FollowUpQuestionsProps {
  onQuestionClick: (question: string) => void;
}

const questions = [
  "What is the monthly distribution of total revenue generated from sales?",
  "What is the distribution of sales by product category?",
  "What is the distribution of customer satisfaction scores?",
  "Show total_amount by category",
];

export function FollowUpQuestions({ onQuestionClick }: FollowUpQuestionsProps) {
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
      <div className="flex items-center gap-2 mb-4">
        <MessageCircle className="w-5 h-5 text-blue-600" />
        <h3 className="text-base font-semibold text-gray-900">Suggested Questions</h3>
      </div>
      
      <div className="flex flex-wrap gap-3">
        {questions.map((question, index) => (
          <button
            key={index}
            onClick={() => onQuestionClick(question)}
            className="px-4 py-2 bg-gray-50 hover:bg-blue-50 border border-gray-200 hover:border-blue-300 rounded-full text-sm text-gray-700 hover:text-blue-700 transition-all"
          >
            {question}
          </button>
        ))}
      </div>
    </div>
  );
}
