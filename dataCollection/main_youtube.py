import warnings
warnings.filterwarnings('ignore')
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
os.environ['PYTHONWARNINGS'] = 'ignore'

from googleapiclient.discovery import build
from datetime import datetime, timedelta
import re
import argparse
from typing import Optional, Dict, List
from tabulate import tabulate

class YouTubeAnalytics:
    def __init__(self, api_key: str):
        """Initialize YouTube API client."""
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def get_channel_id(self, identifier: str) -> Optional[str]:
        """Extract channel ID from username, handle, or channel URL."""
        if 'youtube.com' in identifier:
            if '/channel/' in identifier:
                return identifier.split('/channel/')[1].split('/')[0]
            elif '/c/' in identifier or '/user/' in identifier:
                username = identifier.split('/')[-1]
                return self._get_channel_id_from_username(username)
            elif '/@' in identifier:
                handle = identifier.split('/@')[1].split('/')[0]
                return self._get_channel_id_from_handle(handle)
        elif identifier.startswith('@'):
            return self._get_channel_id_from_handle(identifier[1:])
        else:
            return self._get_channel_id_from_username(identifier)
        return None
    
    def _get_channel_id_from_username(self, username: str) -> Optional[str]:
        """Get channel ID from username."""
        try:
            response = self.youtube.channels().list(
                part='id',
                forUsername=username
            ).execute()
            
            if response['items']:
                return response['items'][0]['id']
        except Exception as e:
            print(f"Error getting channel ID from username: {e}")
        return None
    
    def _get_channel_id_from_handle(self, handle: str) -> Optional[str]:
        """Get channel ID from handle (@username)."""
        try:
            response = self.youtube.search().list(
                part='snippet',
                q=f'@{handle}',
                type='channel',
                maxResults=1
            ).execute()
            
            if response['items']:
                return response['items'][0]['snippet']['channelId']
        except Exception as e:
            print(f"Error getting channel ID from handle: {e}")
        return None
    
    def get_video_details(self, video_id: str) -> Dict:
        """Get detailed statistics for a specific video."""
        try:
            response = self.youtube.videos().list(
                part='statistics,snippet,contentDetails,liveStreamingDetails',
                id=video_id
            ).execute()
            
            if response['items']:
                video = response['items'][0]
                duration = video['contentDetails']['duration']
                
                # Determine video type
                video_type = self._determine_video_type(
                    duration,
                    video.get('liveStreamingDetails'),
                    video['snippet'].get('liveBroadcastContent')
                )
                
                return {
                    'title': video['snippet']['title'],
                    'published_at': video['snippet']['publishedAt'],
                    'views': int(video['statistics'].get('viewCount', 0)),
                    'likes': int(video['statistics'].get('likeCount', 0)),
                    'comments': int(video['statistics'].get('commentCount', 0)),
                    'duration': duration,
                    'type': video_type,
                    'url': f"https://youtube.com/watch?v={video_id}"
                }
        except Exception as e:
            print(f"Error getting video details: {e}")
        return {}
    
    def _determine_video_type(self, duration: str, live_details: Optional[Dict], broadcast_content: str) -> str:
        """Determine if video is Short, Live, or regular Video."""
        if live_details or broadcast_content in ['live', 'upcoming']:
            return 'Live'
        elif self._is_short_duration(duration):
            return 'Short'
        else:
            return 'Video'
    
    def _is_short_duration(self, duration: str) -> bool:
        """Check if video duration is less than 60 seconds (Short)."""
        match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return total_seconds <= 60
        return False
    
    def get_channel_videos(self, channel_id: str, days: int) -> List[Dict]:
        """Get all videos from a channel within specified time period."""
        videos = []
        published_after = (datetime.utcnow() - timedelta(days=days)).isoformat() + 'Z'
        
        try:
            response = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            ).execute()
            
            if not response['items']:
                return videos
            
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            next_page_token = None
            while True:
                playlist_response = self.youtube.playlistItems().list(
                    part='snippet',
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                
                for item in playlist_response['items']:
                    video_published = item['snippet']['publishedAt']
                    if video_published < published_after:
                        continue
                    
                    video_id = item['snippet']['resourceId']['videoId']
                    video_details = self.get_video_details(video_id)
                    if video_details:
                        videos.append({
                            'video_id': video_id,
                            **video_details
                        })
                
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break
                
        except Exception as e:
            print(f"Error fetching channel videos: {e}")
        
        return videos

def format_number(num: int) -> str:
    """Format number with commas and convert to K/M/B if large."""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)

def analyze_and_print_stats(df: pd.DataFrame):
    """Analyze and print detailed statistics."""
    print("\n=== Channel Analytics Summary ===")
    
    # Overall stats
    print("\n📊 Overall Statistics:")
    print(f"Total videos analyzed: {len(df)}")
    print(f"Total views: {format_number(df['views'].sum())}")
    print(f"Total likes: {format_number(df['likes'].sum())}")
    print(f"Total comments: {format_number(df['comments'].sum())}")
    
    # Content type distribution
    print("\n📌 Content Type Distribution:")
    type_dist = df['type'].value_counts()
    print(tabulate([[type_, count] for type_, count in type_dist.items()],
                  headers=['Type', 'Count'], tablefmt='pretty'))
    
    # Average stats by content type
    print("\n📈 Average Performance by Content Type:")
    type_stats = df.groupby('type').agg({
        'views': 'mean',
        'likes': 'mean',
        'comments': 'mean'
    }).round(2)
    
    type_stats_formatted = type_stats.applymap(format_number)
    print(tabulate(type_stats_formatted.reset_index(),
                  headers=['Type', 'Avg Views', 'Avg Likes', 'Avg Comments'],
                  tablefmt='pretty'))
    
    # Top performing videos
    print("\n🏆 Top 5 Videos by Views:")
    top_views = df.nlargest(5, 'views')[['title', 'type', 'views', 'likes', 'url']]
    print(tabulate(top_views.applymap(lambda x: format_number(x) if isinstance(x, (int, float)) else x),
                  headers=['Title', 'Type', 'Views', 'Likes', 'URL'],
                  tablefmt='pretty'))
    
    print("\n❤️ Top 5 Videos by Likes:")
    top_likes = df.nlargest(5, 'likes')[['title', 'type', 'views', 'likes', 'url']]
    print(tabulate(top_likes.applymap(lambda x: format_number(x) if isinstance(x, (int, float)) else x),
                  headers=['Title', 'Type', 'Views', 'Likes', 'URL'],
                  tablefmt='pretty'))
    
    # Engagement rates
    print("\n💫 Engagement Rates:")
    df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views'] * 100).round(2)
    engagement_by_type = df.groupby('type')['engagement_rate'].mean().round(2)
    print(tabulate([[type_, f"{rate}%"] for type_, rate in engagement_by_type.items()],
                  headers=['Content Type', 'Avg Engagement Rate'],
                  tablefmt='pretty'))



def print_top_performers(df: pd.DataFrame, content_type: str, metric: str, n: int = 5):
    """Print top performing videos of specific type by given metric."""
    type_df = df[df['type'] == content_type]
    if len(type_df) == 0:
        print(f"\nNo {content_type}s found in the analyzed period.")
        return
        
    top_videos = type_df.nlargest(n, metric)[['title', 'views', 'likes', 'comments', 'url']]
    formatted_df = top_videos.applymap(lambda x: format_number(x) if isinstance(x, (int, float)) else x)
    
    print(f"\n🏆 Top {n} {content_type}s by {metric}:")
    print(tabulate(formatted_df,
                  headers=['Title', 'Views', 'Likes', 'Comments', 'URL'],
                  tablefmt='pretty'))

def analyze_and_print_stats(df: pd.DataFrame):
    """Analyze and print detailed statistics by content type."""
    print("\n====================================")
    print("📊 Channel Analytics Summary")
    print("====================================")
    
    # Overall stats
    print("\n📈 Overall Channel Statistics:")
    print(f"Total content pieces: {len(df)}")
    print(f"Total views: {format_number(df['views'].sum())}")
    print(f"Total likes: {format_number(df['likes'].sum())}")
    print(f"Total comments: {format_number(df['comments'].sum())}")
    
    # Content type distribution
    print("\n📌 Content Distribution:")
    type_dist = df['type'].value_counts()
    print(tabulate([[type_, count] for type_, count in type_dist.items()],
                  headers=['Type', 'Count'], tablefmt='pretty'))
    
    # Performance by content type
    print("\n📊 Average Performance by Content Type:")
    type_stats = df.groupby('type').agg({
        'views': 'mean',
        'likes': 'mean',
        'comments': 'mean'
    }).round(2)
    
    type_stats_formatted = type_stats.applymap(format_number)
    print(tabulate(type_stats_formatted.reset_index(),
                  headers=['Type', 'Avg Views', 'Avg Likes', 'Avg Comments'],
                  tablefmt='pretty'))
    
    # Engagement rates by type
    print("\n💫 Engagement Rates by Content Type:")
    df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views'] * 100).round(2)
    engagement_by_type = df.groupby('type')['engagement_rate'].mean().round(2)
    print(tabulate([[type_, f"{rate}%"] for type_, rate in engagement_by_type.items()],
                  headers=['Content Type', 'Avg Engagement Rate'],
                  tablefmt='pretty'))
    
    print("\n====================================")
    print("🎯 Top Performers By Category")
    print("====================================")
    
    # Analyze each content type separately
    for content_type in df['type'].unique():
        print(f"\n=== {content_type} Analytics ===")
        
        # Top by views
        print_top_performers(df, content_type, 'views')
        
        # Top by likes
        print_top_performers(df, content_type, 'likes')
        
        # Top by comments
        print_top_performers(df, content_type, 'comments')
        
        # Type-specific stats
        type_df = df[df['type'] == content_type]
        total_views = type_df['views'].sum()
        avg_views = type_df['views'].mean()
        view_share = (total_views / df['views'].sum() * 100).round(2)
        
        print(f"\n📊 {content_type} Statistics:")
        print(f"Total {content_type}s: {len(type_df)}")
        print(f"Total Views: {format_number(total_views)}")
        print(f"Average Views: {format_number(avg_views)}")
        print(f"Share of Total Views: {view_share}%")
        
        # Calculate peak performance times
        if len(type_df) > 0:
            type_df['published_at'] = pd.to_datetime(type_df['published_at'])
            type_df['day_of_week'] = type_df['published_at'].dt.day_name()
            type_df['hour'] = type_df['published_at'].dt.hour
            
            best_day = type_df.groupby('day_of_week')['views'].mean().idxmax()
            best_hour = type_df.groupby('hour')['views'].mean().idxmax()
            
            print(f"\n⏰ Best Publishing Times for {content_type}s:")
            print(f"Best Day: {best_day}")
            print(f"Best Hour: {best_hour:02d}:00")
        
        print("\n-----------------------------------")

def main():
    parser = argparse.ArgumentParser(description='YouTube Channel Analytics')
    parser.add_argument('identifier', help='YouTube channel URL, username, or @handle')
    parser.add_argument('--api-key', required=True, help='YouTube Data API key')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze (default: 30)')
    parser.add_argument('--output', default='youtube_analytics.csv', help='Output CSV file name')
    
    args = parser.parse_args()
    
    analyzer = YouTubeAnalytics(args.api_key)
    
    print(f"📱 Analyzing channel: {args.identifier}")
    channel_id = analyzer.get_channel_id(args.identifier)
    if not channel_id:
        print("❌ Could not find channel ID. Please check the URL, username, or handle.")
        return
    
    print(f"🔄 Fetching videos from the past {args.days} days...")
    videos = analyzer.get_channel_videos(channel_id, args.days)
    
    if not videos:
        print("❌ No videos found in the specified time period.")
        return
    
    df = pd.DataFrame(videos)
    df.to_csv(args.output, index=False)
    print(f"💾 Raw data saved to {args.output}")
    
    analyze_and_print_stats(df)

if __name__ == "__main__":
    main()