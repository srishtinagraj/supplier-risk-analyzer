import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Supplier Risk Intelligence",
    page_icon="📊",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Create Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    """Create and cache Snowflake connection"""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

# Query functions
@st.cache_data(ttl=300)
def get_supplier_risk_scores():
    """Fetch supplier risk scores from view"""
    conn = get_snowflake_connection()
    query = "SELECT * FROM V_SUPPLIER_RISK_SCORE ORDER BY AVG_SENTIMENT_SCORE ASC"
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=300)
def get_recent_alerts():
    """Fetch recent negative communications"""
    conn = get_snowflake_connection()
    query = "SELECT * FROM V_RECENT_ALERTS LIMIT 10"
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=300)
def get_sentiment_trend():
    """Get sentiment trend over time"""
    conn = get_snowflake_connection()
    query = """
    SELECT 
        DATE_TRUNC('day', sc.COMMUNICATION_DATE) as DATE,
        AVG(sa.SENTIMENT_SCORE) as AVG_SENTIMENT,
        COUNT(*) as COMM_COUNT
    FROM SUPPLIER_COMMUNICATIONS sc
    JOIN SENTIMENT_ANALYSIS sa ON sc.COMM_ID = sa.COMM_ID
    GROUP BY DATE_TRUNC('day', sc.COMMUNICATION_DATE)
    ORDER BY DATE
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=300)
def get_category_analysis():
    """Get risk by supplier category"""
    conn = get_snowflake_connection()
    query = """
    SELECT 
        s.CATEGORY,
        COUNT(DISTINCT s.SUPPLIER_ID) as SUPPLIER_COUNT,
        AVG(sa.SENTIMENT_SCORE) as AVG_SENTIMENT,
        SUM(CASE WHEN sa.SENTIMENT_LABEL = 'NEGATIVE' THEN 1 ELSE 0 END) as NEGATIVE_COUNT
    FROM SUPPLIERS s
    JOIN SENTIMENT_ANALYSIS sa ON s.SUPPLIER_ID = sa.SUPPLIER_ID
    GROUP BY s.CATEGORY
    ORDER BY AVG_SENTIMENT ASC
    """
    df = pd.read_sql(query, conn)
    return df

# Main app
def main():
    # Header
    st.title("🔍 Supplier Risk Intelligence Dashboard")
    st.markdown("*Powered by Snowflake Cortex ML*")
    st.markdown("---")
    
    # Fetch data
    try:
        risk_scores = get_supplier_risk_scores()
        alerts = get_recent_alerts()
        trend_data = get_sentiment_trend()
        category_data = get_category_analysis()
        
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            high_risk_count = len(risk_scores[risk_scores['RISK_CATEGORY'] == 'HIGH_RISK'])
            st.metric("🔴 High Risk Suppliers", high_risk_count)
        
        with col2:
            avg_sentiment = risk_scores['AVG_SENTIMENT_SCORE'].mean()
            st.metric("📊 Avg Sentiment Score", f"{avg_sentiment:.2f}")
        
        with col3:
            total_comms = risk_scores['TOTAL_COMMUNICATIONS'].sum()
            st.metric("📧 Total Communications", int(total_comms))
        
        with col4:
            negative_comms = risk_scores['NEGATIVE_COUNT'].sum()
            st.metric("⚠️ Negative Alerts", int(negative_comms))
        
        st.markdown("---")
        
        # Two column layout
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            # Supplier risk table
            st.subheader("📋 Supplier Risk Scores")
            
            # Color code risk categories
            def color_risk(val):
                if val == 'HIGH_RISK':
                    return 'background-color: #ffcccc'
                elif val == 'MEDIUM_RISK':
                    return 'background-color: #fff4cc'
                else:
                    return 'background-color: #ccffcc'
            
            styled_df = risk_scores.style.applymap(
                color_risk, 
                subset=['RISK_CATEGORY']
            ).format({
                'AVG_SENTIMENT_SCORE': '{:.3f}'
            })
            
            st.dataframe(styled_df, use_container_width=True, height=400)
        
        with col_right:
            # Risk distribution pie chart
            st.subheader("🎯 Risk Distribution")
            risk_dist = risk_scores['RISK_CATEGORY'].value_counts()
            
            fig_pie = px.pie(
                values=risk_dist.values,
                names=risk_dist.index,
                color=risk_dist.index,
                color_discrete_map={
                    'HIGH_RISK': '#ff6b6b',
                    'MEDIUM_RISK': '#ffd93d',
                    'LOW_RISK': '#6bcf7f'
                }
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)
        
        st.markdown("---")
        
        # Sentiment trend chart
        st.subheader("📈 Sentiment Trend Over Time")
        fig_trend = go.Figure()
        
        fig_trend.add_trace(go.Scatter(
            x=trend_data['DATE'],
            y=trend_data['AVG_SENTIMENT'],
            mode='lines+markers',
            name='Avg Sentiment',
            line=dict(color='#4ecdc4', width=3),
            marker=dict(size=8)
        ))
        
        fig_trend.add_hline(y=0, line_dash="dash", line_color="gray", 
                           annotation_text="Neutral")
        fig_trend.add_hline(y=-0.3, line_dash="dash", line_color="red", 
                           annotation_text="Negative Threshold")
        fig_trend.add_hline(y=0.3, line_dash="dash", line_color="green", 
                           annotation_text="Positive Threshold")
        
        fig_trend.update_layout(
            xaxis_title="Date",
            yaxis_title="Sentiment Score",
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig_trend, use_container_width=True)
        
        st.markdown("---")
        
        # Category analysis
        col_cat1, col_cat2 = st.columns(2)
        
        with col_cat1:
            st.subheader("📦 Risk by Category")
            fig_cat = px.bar(
                category_data,
                x='CATEGORY',
                y='AVG_SENTIMENT',
                color='AVG_SENTIMENT',
                color_continuous_scale=['red', 'yellow', 'green'],
                text='AVG_SENTIMENT'
            )
            fig_cat.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig_cat.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_cat, use_container_width=True)
        
        with col_cat2:
            st.subheader("⚠️ Negative Communications by Category")
            fig_neg = px.bar(
                category_data,
                x='CATEGORY',
                y='NEGATIVE_COUNT',
                color='NEGATIVE_COUNT',
                color_continuous_scale='Reds',
                text='NEGATIVE_COUNT'
            )
            fig_neg.update_traces(textposition='outside')
            fig_neg.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_neg, use_container_width=True)
        
        st.markdown("---")
        
        # Recent alerts section
        st.subheader("🚨 Recent Negative Alerts")
        
        for idx, row in alerts.iterrows():
            with st.expander(f"⚠️ {row['SUPPLIER_NAME']} - {row['SUBJECT']} ({row['COMMUNICATION_DATE'].strftime('%Y-%m-%d')})"):
                col_a, col_b = st.columns([1, 3])
                
                with col_a:
                    st.metric("Sentiment Score", f"{row['SENTIMENT_SCORE']:.3f}")
                    st.write(f"**Source:** {row['SOURCE_TYPE']}")
                
                with col_b:
                    st.write("**Summary:**")
                    st.write(row['KEY_PHRASES'])
        
        # Footer
        st.markdown("---")
        st.markdown("*Built with Snowflake Cortex ML | Data refreshes every 5 minutes*")
        
    except Exception as e:
        st.error(f"❌ Error connecting to Snowflake: {str(e)}")
        st.info("Please check your .env file and ensure Snowflake credentials are correct.")

if __name__ == "__main__":
    main()