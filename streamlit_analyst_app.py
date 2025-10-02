"""Ultra simple chat - refactored with event stream architecture."""

from tracemalloc import stop
import streamlit as st
import logging
from src.config import get_config, get_mcp_config, setup_environment_variables, get_auth_config
from src.constants import PROJ_ENDPOINT_KEY, AGENT_ID_KEY
from src.mcp_client import get_mcp_token_sync, display_mcp_status
from src.auth import initialize_msal_auth
from src.agent_manager import AgentManager
from src.run_processor import RunProcessor
from src.event_renderer import EventRenderer, render_error_buttons
from src.run_events import RequiresApprovalEvent, MessageEvent, ErrorEvent, ToolCallEvent, ToolCallsStepEvent
from src.orchestrator_agent import OrchestratorAgent, WorkflowStep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def on_tool_approve(event: RequiresApprovalEvent, agent_manager: AgentManager):
    """Handle tool approval."""
    if agent_manager.submit_approvals(event, approved=True):
        # Unblock processor and continue
        if 'processor' in st.session_state and st.session_state.processor:
            st.session_state.processor.unblock()
        st.session_state.pending_approval = None
        st.session_state.stage = 'processing'


def on_tool_deny(event: RequiresApprovalEvent, agent_manager: AgentManager):
    """Handle tool denial."""
    if agent_manager.submit_approvals(event, approved=False):
        # Denied - stop processing
        st.session_state.pending_approval = None
        st.session_state.processor = None
        st.session_state.stage = 'user_input'


def on_error_retry():
    """Handle error retry."""
    st.session_state.stage = 'processing'
    st.session_state.error_event = None


def on_error_cancel():
    """Handle error cancel."""
    st.session_state.stage = 'user_input'
    st.session_state.run_id = None
    st.session_state.processor = None
    st.session_state.error_event = None

def render_message_history():
    """Render message history from session state."""
    for item in st.session_state.messages:
        if isinstance(item, dict):
            # User message - simple dict
            with st.chat_message(item["role"]):
                st.markdown(item["content"])
        else:
            # Assistant event - RunEvent object
            with st.chat_message("assistant"):
                EventRenderer.render(item)


def initialize_app() -> AgentManager:
    """
    Initialize application: config, auth, MCP, agent manager, session state.
    Returns AgentManager instance.
    """
    # Get configuration
    config = get_config()
    if not config:
        st.error("❌ Please configure your Azure AI Foundry settings in Streamlit secrets.")
        st.stop()
    
    # Setup environment
    setup_environment_variables()
    
    # Get authentication configuration
    client_id, tenant_id, _ = get_auth_config()
    if not client_id or not tenant_id:
        st.stop()
    
    # Initialize MSAL authentication in sidebar
    with st.sidebar:
        token_credential = initialize_msal_auth(client_id, tenant_id)
    
    # Check if user is authenticated
    if not token_credential:
        st.error("❌ Please sign in to use the chatbot.")
        st.stop()
    
    # Get MCP configuration and token
    mcp_config = get_mcp_config()
    mcp_token = get_mcp_token_sync(mcp_config)
    
    # Display MCP status in sidebar
    # Get approval setting (default to True)
    require_approval = True
    
    if mcp_config:
        with st.sidebar:
            display_mcp_status(mcp_config, mcp_token)
            # Add approval setting inside MCP section
            st.divider()
            require_approval = st.checkbox(
                "Require tool approval", 
                value=True,
                help="When enabled, you'll need to approve each tool call before execution"
            )
    
    # Initialize agent manager
    agent_manager = AgentManager(
        project_endpoint=config[PROJ_ENDPOINT_KEY],
        agent_id=config[AGENT_ID_KEY],
        mcp_config=mcp_config,
        mcp_token=mcp_token,
        require_approval=require_approval
    )
    
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = None
    if "processor" not in st.session_state:
        st.session_state.processor = None
    if 'stage' not in st.session_state:
        st.session_state.stage = 'user_input'
    if 'run_id' not in st.session_state:
        st.session_state.run_id = None
    if 'pending_approval' not in st.session_state:
        st.session_state.pending_approval = None
    if 'error_event' not in st.session_state:
        st.session_state.error_event = None
    
    # Initialize orchestrator agent
    if 'orchestrator_agent' not in st.session_state:
        st.session_state.orchestrator_agent = OrchestratorAgent()
    
    # Create thread if needed
    if not st.session_state.thread_id:
        st.session_state.thread_id = agent_manager.create_thread()
    
    return agent_manager


def render_workflow_status():
    """Render workflow status in sidebar."""
    orchestrator = st.session_state.orchestrator_agent
    status = orchestrator.get_workflow_status()
    
    if status["status"] == "no_workflow":
        return
    
    st.sidebar.divider()
    st.sidebar.subheader("📊 Data Analysis Workflow")
    
    # Define steps in order
    steps = [
        ("build_query", "1️⃣ Build Query"),
        ("validate_query", "2️⃣ Validate Query"),
        ("execute_query", "3️⃣ Execute Query"),
        ("verify_results", "4️⃣ Verify Results"),
        ("format_results", "5️⃣ Format Report")
    ]
    
    # Show current step
    current_step = status.get("current_step", "")
    is_complete = status.get("is_complete", False) or st.session_state.get("workflow_complete", False)
    
    for step_id, step_label in steps:
        if step_id == current_step and not is_complete:
            st.sidebar.markdown(f"**▶️ {step_label}**")
        elif step_id in status.get("step_history", []) or is_complete:
            st.sidebar.markdown(f"✅ {step_label}")
        else:
            st.sidebar.markdown(f"⚪ {step_label}")
    
    # Show completion status
    if is_complete:
        st.sidebar.success("🎉 Workflow Complete!")
    
    # Show stats
    st.sidebar.caption(f"Tool calls: {status.get('total_tool_calls', 0)}")
    
    # Reset button
    if st.sidebar.button("🔄 Start New Analysis"):
        st.session_state.orchestrator_agent = OrchestratorAgent()
        st.session_state.messages = []
        st.session_state.thread_id = None
        st.session_state.workflow_complete = False
        st.rerun()


def main():
    st.title("🤖 Data Analyst Chat")
    
    # Initialize app (config, auth, MCP, agent manager, session state, thread)
    agent_manager = initialize_app()
    
    # Render workflow status
    render_workflow_status()
    
    # Display message history
    render_message_history()
    
    # Handle pending approval (blocking state)
    if st.session_state.pending_approval:
        event = st.session_state.pending_approval
        with st.chat_message("assistant"):
            EventRenderer.render_approval_request(event,
                                                lambda e: on_tool_approve(e, agent_manager),
                                                lambda e: on_tool_deny(e, agent_manager))
        return
    
    # Handle error state
    if st.session_state.stage == 'error' and 'error_event' in st.session_state:
        error_event = st.session_state.error_event
        with st.chat_message("assistant"):
            EventRenderer.render_error(error_event)
            render_error_buttons(on_error_retry, on_error_cancel)
        return
    
    # Handle user input
    if st.session_state.stage == 'user_input':
        if prompt := st.chat_input("Ask a question about the data:"):
            # User message - simple dict (not an event)
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            with st.chat_message("user"):
                st.markdown(prompt)
            
            with st.spinner("Thinking...", show_time=True):
                # Start workflow with orchestrator
                orchestrator = st.session_state.orchestrator_agent
                context = orchestrator.start_workflow(prompt)
                
                # Get prompt for current step
                step_prompt = orchestrator.get_current_step_prompt()
                
                # Create run with step-specific prompt
                run_id = agent_manager.create_run(st.session_state.thread_id, step_prompt)
                st.session_state.run_id = run_id
                st.session_state.processor = RunProcessor(agent_manager.agents_client)
                st.session_state.stage = 'processing'
    
    # Process run events
    if st.session_state.stage == 'processing' and st.session_state.run_id:
        processor = st.session_state.processor
        
        if not processor:
            logger.error("No processor in session state!")
            st.error("Error: No processor found")
            st.session_state.stage = 'user_input'
            return
        
        with st.chat_message("assistant"):
            event_generator = processor.poll_run_events(
                thread_id=st.session_state.thread_id,
                run_id=st.session_state.run_id
            )

            events_exhausted = False
            
            while not events_exhausted:
                event = None
                with st.spinner("Analyzing...", show_time=True):
                    try:
                        event = next(event_generator)
                    except StopIteration as e:
                        events_exhausted = True
                        continue;
                
                logger.info(f"📦 Received event: {event.event_type} (id: {event.event_id})")
                
                # Handle blocking event
                if event.is_blocking:
                    # Check if auto-approval is enabled
                    if not agent_manager.require_approval:

                        on_tool_approve(event, agent_manager)
                        st.markdown(f"Executing tool **{event.tool_calls[0].name}**...")
                        st.rerun()
                        return

                    else:
                        # Show approval dialog
                        st.session_state.pending_approval = event
                        st.rerun()
                        return
                
                # Render event - use typing effect for new messages only
                if isinstance(event, MessageEvent):
                    EventRenderer.render_message_with_typing(event)
                else:
                    EventRenderer.render(event)
                
                # Handle error events
                if isinstance(event, ErrorEvent):
                    st.session_state.error_event = event
                    st.session_state.stage = 'error'
                    st.rerun()
                    return
                
                # Store event in history (skip completion/error events)
                if event.event_type not in ['completed', 'error']:
                    st.session_state.messages.append(event)
                
                # Track tool calls in orchestrator
                if isinstance(event, ToolCallEvent):
                    orchestrator = st.session_state.orchestrator_agent
                    orchestrator.add_tool_call(event.tool_name)
                    logger.info(f"📝 Tracked tool call: {event.tool_name}")
                
                elif isinstance(event, ToolCallsStepEvent):
                    orchestrator = st.session_state.orchestrator_agent
                    for tool_call in event.tool_calls:
                        orchestrator.add_tool_call(tool_call.tool_name)
                    logger.info(f"📝 Tracked {len(event.tool_calls)} tool call(s) from step")
                
                # Also track tool calls from other event types that might contain them
                elif hasattr(event, 'tool_calls') and event.tool_calls:
                    orchestrator = st.session_state.orchestrator_agent
                    for tool_call in event.tool_calls:
                        if hasattr(tool_call, 'tool_name'):
                            orchestrator.add_tool_call(tool_call.tool_name)
                        elif hasattr(tool_call, 'name'):
                            orchestrator.add_tool_call(tool_call.name)
                    logger.info(f"📝 Tracked {len(event.tool_calls)} tool call(s) from {type(event).__name__}")
        
        # Run completed - check orchestrator workflow
        logger.info("✅ Run completed")
        
        orchestrator = st.session_state.orchestrator_agent
        
        # Analyze and decide next step
        decision = orchestrator.analyze_and_decide_next()
        logger.info(f"🧠 Orchestrator decision: {decision}")
        
        # Debug: show orchestrator context
        status = orchestrator.get_workflow_status()
        logger.info(f"🔍 Orchestrator status: {status}")
        
        if decision == "workflow_complete":
            logger.info("🎉 Workflow complete!")
            
            # Mark workflow as complete
            st.session_state.workflow_complete = True
            
            # Reset to user input
            st.session_state.stage = 'user_input'
            st.session_state.run_id = None
            st.session_state.processor = None
            st.rerun()
        
        elif decision in ["move_to_validate", "move_to_execute", "move_to_verify", "move_to_format", "move_to_build_query"]:
            # Move to next step
            orchestrator.move_to_next_step(decision)
            
            # Get prompt for next step
            next_prompt = orchestrator.get_current_step_prompt()
            
            # Add transition message
            step_names = {
                "move_to_validate": "Validate Query",
                "move_to_execute": "Execute Query", 
                "move_to_verify": "Verify Results",
                "move_to_build_query": "Build Query"
            }
            transition_msg = f"➡️ Moving to next step: **{step_names.get(decision, decision)}**"
            st.session_state.messages.append({
                "role": "assistant", 
                "content": transition_msg
            })
            
            # Create new run for next step
            logger.info(f"🔄 Auto-continuing to next step")
            run_id = agent_manager.create_run(st.session_state.thread_id, next_prompt)
            st.session_state.run_id = run_id
            st.session_state.processor = RunProcessor(agent_manager.agents_client)
            st.session_state.stage = 'processing'
            st.rerun()
        
        elif decision == "retry_current":
            # Retry current step with reminder
            logger.warning(f"⚠️ Retrying current step")
            
            retry_prompt = f"""IMPORTANT: You need to complete the current task.

{orchestrator.get_current_step_prompt()}

Please use the available tools to complete this task.
"""
            
            # Create new run with reminder
            run_id = agent_manager.create_run(st.session_state.thread_id, retry_prompt)
            st.session_state.run_id = run_id
            st.session_state.processor = RunProcessor(agent_manager.agents_client)
            st.session_state.stage = 'processing'
            st.rerun()
        
        else:
            # Unknown decision, reset to user input
            logger.warning(f"⚠️ Unknown decision: {decision}, resetting to user input")
            st.session_state.stage = 'user_input'
            st.session_state.run_id = None
            st.session_state.processor = None
            st.rerun()


if __name__ == "__main__":
    main()
