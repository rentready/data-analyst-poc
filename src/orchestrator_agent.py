"""Orchestrator Agent - manages the data analysis workflow."""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class WorkflowStep(Enum):
    """Steps in the data analysis workflow."""
    BUILD_QUERY = "build_query"
    VALIDATE_QUERY = "validate_query" 
    EXECUTE_QUERY = "execute_query"
    VERIFY_RESULTS = "verify_results"
    EXPLORE_DATA = "explore_data"


@dataclass
class WorkflowContext:
    """Context for the workflow."""
    user_query: str
    current_step: WorkflowStep
    step_history: List[WorkflowStep]
    shared_data: Dict[str, Any]
    tool_calls: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'user_query': self.user_query,
            'current_step': self.current_step.value,
            'step_history': [step.value for step in self.step_history],
            'shared_data': self.shared_data,
            'tool_calls': self.tool_calls
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowContext':
        """Deserialize from dictionary."""
        return cls(
            user_query=data['user_query'],
            current_step=WorkflowStep(data['current_step']),
            step_history=[WorkflowStep(step) for step in data['step_history']],
            shared_data=data.get('shared_data', {}),
            tool_calls=data.get('tool_calls', [])
        )


class OrchestratorAgent:
    """Agent that orchestrates the data analysis workflow."""
    
    def __init__(self):
        self.context: Optional[WorkflowContext] = None
    
    def start_workflow(self, user_query: str) -> WorkflowContext:
        """Start a new workflow."""
        self.context = WorkflowContext(
            user_query=user_query,
            current_step=WorkflowStep.BUILD_QUERY,
            step_history=[],
            shared_data={},
            tool_calls=[]
        )
        logger.info(f"🚀 Started workflow for query: {user_query}")
        return self.context
    
    def get_current_step_prompt(self) -> str:
        """Get the prompt for the current step."""
        if not self.context:
            return "No active workflow."
        
        step_prompts = {
            WorkflowStep.BUILD_QUERY: self._get_build_query_prompt(),
            WorkflowStep.VALIDATE_QUERY: self._get_validate_query_prompt(),
            WorkflowStep.EXECUTE_QUERY: self._get_execute_query_prompt(),
            WorkflowStep.VERIFY_RESULTS: self._get_verify_results_prompt(),
            WorkflowStep.EXPLORE_DATA: self._get_explore_data_prompt()
        }
        
        return step_prompts.get(self.context.current_step, "Unknown step.")
    
    def _get_build_query_prompt(self) -> str:
        """Get prompt for build query step."""
        return f"""You are a data analyst for RentReady system. User asked: "{self.context.user_query}"

CURRENT TASK: BUILD PRELIMINARY SQL QUERY (Step 1 of 4)

You have access to MCP tools that can query the RentReady SQL Server database. The database contains:
- Work orders (msdyn_workorder table) with dates, status, service accounts
- Job profiles (rr_jobprofile table) linked to work orders  
- Invoices (invoice table) with billing information
- Accounts (account table) for properties and customers
- Work order services (msdyn_workorderservice table) with service details

YOUR TASK - BUILD THE QUERY NOW:
1. Analyze the user's question
2. Determine which table(s) you need (work orders, invoices, job profiles, etc.)
3. BUILD a preliminary SQL query using read_data or find_* MCP tools
4. Include appropriate filters (dates, status, etc.)

IMPORTANT RULES:
- DO NOT ask the user for more information - you have enough to start
- DO NOT wait - build the query NOW based on your understanding
- Use your knowledge of the RentReady schema
- This is preliminary - it will be tested and refined in the next step
- If you're not 100% sure, make your best guess and we'll validate with samples

EXAMPLE: If user asks "How many work orders in September 2024?", you should:
- Use find_work_orders or read_data on msdyn_workorder table
- Filter by date_from="2024-09-01" and date_to="2024-09-30"
- Count the results

NOW BUILD THE PRELIMINARY QUERY for the user's question above. Show the query/tool call and explain your reasoning.
"""
    
    def _get_validate_query_prompt(self) -> str:
        """Get prompt for validate query step."""
        return f"""You are a data analyst. User asked: "{self.context.user_query}"

CURRENT TASK: QUERY TESTING AND REFINEMENT (Step 2 of 4)

You already built a preliminary SQL query. Now you need to TEST and REFINE it.

Your task:
1. TEST the query against REAL DATA SAMPLES (use LIMIT to get small samples)
2. Analyze the sample results - do they make sense?
3. Look at actual tables and fields if needed
4. Refine the query based on what you learn from the samples
5. This is an ITERATIVE process - test, analyze, refine, repeat

IMPORTANT:
- Use read_data, find_work_orders, find_invoices, etc. with LIMIT to get samples
- Check if the data structure matches your assumptions
- Verify field names, data types, and relationships
- If the query doesn't work, fix it and test again
- Don't proceed until you're confident the query is correct

This is where you explore the data through sampling to validate your query.
"""
    
    def _get_execute_query_prompt(self) -> str:
        """Get prompt for execute query step."""
        return f"""You are a data analyst. User asked: "{self.context.user_query}"

CURRENT TASK: FULL QUERY EXECUTION (Step 3 of 4)

You already built, tested, and refined the SQL query against real data samples.

Your task:
1. Execute the full SQL query using appropriate tools (read_data, find_* etc.)
2. Get the complete results for the entire dataset
3. Handle possible execution errors
4. Ensure you get all the data needed to answer the user's question

The query has already been tested on samples, so it should work correctly.

If errors occur during execution:
- Analyze the cause (runtime error, wrong data, etc.)
- Decide if you need to go back to query refinement or just retry

After execution:
- Show the results obtained
- Report the number of rows and key statistics
- Confirm you have all the data needed to answer the question
"""
    
    def _get_verify_results_prompt(self) -> str:
        """Get prompt for verify results step."""
        return f"""You are a data analyst. User asked: "{self.context.user_query}"

CURRENT TASK: RESULTS VERIFICATION (Final Step 4 of 4)

You executed the SQL query and got results.

Your task:
1. Analyze the obtained results thoroughly
2. Ensure they directly answer the original user question
3. Check that data looks correct and makes business sense
4. Verify the results are complete and accurate
5. Formulate a clear, comprehensive answer for the user

If results don't answer the question or look incorrect:
- Determine the cause (wrong query, wrong data, etc.)
- Decide which step to go back to if needed

If everything is correct:
- Formulate a clear, detailed answer to the user's question
- Provide key numbers, insights, and business context
- Visualize results if helpful
- Explain any limitations or assumptions
"""
    
    def _get_explore_data_prompt(self) -> str:
        """Get prompt for explore data step."""
        return f"""You are a data analyst. User asked: "{self.context.user_query}"

CURRENT TASK: DEEP DATA EXPLORATION (Fallback Step)

You need to explore the data structure to understand what's available.

Your task:
1. List available tables using list_tables
2. Take samples from relevant tables using read_data with LIMIT
3. Analyze the data structure and relationships
4. Understand the business logic and data flow
5. Find the correct tables and fields for the user's question

IMPORTANT:
- DO NOT BUILD QUERY YET! Only explore the data.
- Use read_data, find_work_orders, find_invoices, etc. to get samples
- Look at actual data to understand the structure
- This is a fallback step when you need more data context

After exploration, you'll go back to building the query with better understanding.
"""
    
    def add_tool_call(self, tool_name: str, tool_data: Dict[str, Any] = None):
        """Add a tool call to the context."""
        if not self.context:
            return
        
        self.context.tool_calls.append({
            'name': tool_name,
            'step': self.context.current_step.value,
            'data': tool_data or {}
        })
        logger.info(f"📝 Added tool call: {tool_name} in step {self.context.current_step.value}")
    
    def analyze_and_decide_next(self) -> str:
        """Analyze current step and decide what to do next."""
        if not self.context:
            return "no_workflow"
        
        current_step = self.context.current_step
        tool_calls = self.context.tool_calls
        
        # Get tool calls for current step
        current_step_tools = [tc for tc in tool_calls if tc.get('step') == current_step.value]
        
        logger.info(f"🧠 Analyzing step {current_step.value} with {len(current_step_tools)} tool calls")
        logger.info(f"🧠 Total tool calls: {len(tool_calls)}")
        logger.info(f"🧠 Tool calls: {[tc.get('name') for tc in tool_calls]}")
        logger.info(f"🧠 Current step tools: {[tc.get('name') for tc in current_step_tools]}")
        
        # Decision logic for each step
        if current_step == WorkflowStep.BUILD_QUERY:
            if len(current_step_tools) > 0:
                return "move_to_validate"
            else:
                return "retry_current"
        
        elif current_step == WorkflowStep.VALIDATE_QUERY:
            if len(current_step_tools) > 0:
                return "move_to_execute"
            else:
                return "retry_current"
        
        elif current_step == WorkflowStep.EXECUTE_QUERY:
            if len(current_step_tools) > 0:
                return "move_to_verify"
            else:
                return "retry_current"
        
        elif current_step == WorkflowStep.VERIFY_RESULTS:
            # If we have any tool calls from previous steps, we can complete
            if len(tool_calls) > 0:
                return "workflow_complete"
            # Also complete if we've already done the main workflow steps
            elif len(self.context.step_history) >= 3:  # build_query, validate_query, execute_query
                return "workflow_complete"
            else:
                return "retry_current"
        
        elif current_step == WorkflowStep.EXPLORE_DATA:
            if len(current_step_tools) > 0:
                return "move_to_build_query"
            else:
                return "retry_current"
        
        return "retry_current"
    
    def move_to_next_step(self, decision: str):
        """Move to the next step based on decision."""
        if not self.context:
            return
        
        if decision == "move_to_validate":
            self.context.step_history.append(self.context.current_step)
            self.context.current_step = WorkflowStep.VALIDATE_QUERY
            logger.info("➡️ Moving to VALIDATE_QUERY")
        
        elif decision == "move_to_execute":
            self.context.step_history.append(self.context.current_step)
            self.context.current_step = WorkflowStep.EXECUTE_QUERY
            logger.info("➡️ Moving to EXECUTE_QUERY")
        
        elif decision == "move_to_verify":
            self.context.step_history.append(self.context.current_step)
            self.context.current_step = WorkflowStep.VERIFY_RESULTS
            logger.info("➡️ Moving to VERIFY_RESULTS")
        
        elif decision == "move_to_build_query":
            self.context.step_history.append(self.context.current_step)
            self.context.current_step = WorkflowStep.BUILD_QUERY
            logger.info("➡️ Moving to BUILD_QUERY")
        
        elif decision == "workflow_complete":
            logger.info("🎉 Workflow complete!")
        
        elif decision == "retry_current":
            logger.info(f"🔄 Retrying {self.context.current_step.value}")
    
    def is_workflow_complete(self) -> bool:
        """Check if workflow is complete."""
        return self.context is None or len(self.context.step_history) >= 4
    
    def get_workflow_status(self) -> Dict[str, Any]:
        """Get current workflow status."""
        if not self.context:
            return {"status": "no_workflow"}
        
        return {
            "status": "active",
            "current_step": self.context.current_step.value,
            "step_history": [step.value for step in self.context.step_history],
            "total_tool_calls": len(self.context.tool_calls),
            "is_complete": self.is_workflow_complete()
        }
