from googleapiclient.discovery import build
from datetime import datetime, timedelta
import re
from typing import Optional, Dict, List
import json
import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

class YouTubeAnalyzer:
    def __init__(self, api_key: str):
        """Initialize YouTube API client."""
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    

    def get_empty_type_stats(self) -> Dict:
        """Return empty statistics structure for a content type."""
        return {
            "count": 0,
            "total_views": 0,
            "total_likes": 0,
            "total_comments": 0,
            "average_views": 0,
            "average_likes": 0,
            "average_comments": 0,
            "engagement_rate": 0,
            "top_videos": []
        }
    

    def analyze_channel(self, channel_url: str, days: int) -> Dict:
        """
        Analyze a YouTube channel and return structured JSON data.
        
        Args:
            channel_url (str): URL of the YouTube channel
            days (int): Number of past days to analyze
            
        Returns:
            dict: Structured analytics data in JSON format
        """
        channel_id = self.get_channel_id(channel_url)
        if not channel_id:
            return {"error": "Could not find channel ID"}
        
        videos = self.get_channel_videos(channel_id, days)
        if not videos:
            videos = []  # Ensure empty list instead of None
        
        # Calculate analytics
        total_stats = {
            "total_videos": len(videos),
            "total_views": sum(v.get('views', 0) for v in videos),
            "total_likes": sum(v.get('likes', 0) for v in videos),
            "total_comments": sum(v.get('comments', 0) for v in videos)
        }
        
        # Initialize content type stats with zero values
        content_type_stats = {
            "Video": {
                "count": 0,
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "average_views": 0,
                "average_likes": 0,
                "average_comments": 0,
                "engagement_rate": 0,
                "top_videos": []
            },
            "Short": {
                "count": 0,
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "average_views": 0,
                "average_likes": 0,
                "average_comments": 0,
                "engagement_rate": 0,
                "top_videos": []
            },
            "Live": {
                "count": 0,
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "average_views": 0,
                "average_likes": 0,
                "average_comments": 0,
                "engagement_rate": 0,
                "top_videos": []
            }
        }
        
        # Group videos by type and calculate stats
        for video in videos:
            video_type = video.get('type', 'Video')  # Default to 'Video' if type is missing
            stats = content_type_stats[video_type]
            
            stats["count"] += 1
            stats["total_views"] += video.get('views', 0)
            stats["total_likes"] += video.get('likes', 0)
            stats["total_comments"] += video.get('comments', 0)
            
            # Update top videos
            stats["top_videos"].append(video)
            stats["top_videos"] = sorted(
                stats["top_videos"],
                key=lambda x: x.get('views', 0),
                reverse=True
            )[:5]
        
        # Calculate averages and engagement rates
        for stats in content_type_stats.values():
            if stats["count"] > 0:
                stats["average_views"] = round(stats["total_views"] / stats["count"], 2)
                stats["average_likes"] = round(stats["total_likes"] / stats["count"], 2)
                stats["average_comments"] = round(stats["total_comments"] / stats["count"], 2)
                total_engagements = stats["total_likes"] + stats["total_comments"]
                stats["engagement_rate"] = round((total_engagements / stats["total_views"] * 100), 2) if stats["total_views"] > 0 else 0
        
        # Construct final JSON response
        analysis_results = {
            "channel_id": channel_id,
            "analysis_period_days": days,
            "analysis_date": datetime.utcnow().isoformat(),
            "overall_stats": total_stats,
            "content_type_analysis": content_type_stats,
            "all_videos": videos
        }
        
        return analysis_results

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

def get_channel_analytics(channel_url: str, api_key: str, days: int = 30) -> str:
    """
    Main function to get channel analytics in JSON format.
    
    Args:
        channel_url (str): YouTube channel URL
        api_key (str): YouTube Data API key
        days (int): Number of past days to analyze (default: 30)
        
    Returns:
        str: JSON string containing channel analytics
    """
    analyzer = YouTubeAnalyzer(api_key)
    results = analyzer.analyze_channel(channel_url, days)
    return json.dumps(results, indent=2)

# Example usage:
if __name__ == "__main__":
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    CHANNEL_URL = "https://www.youtube.com/@channelname"
    
    analytics_json = get_channel_analytics(CHANNEL_URL, API_KEY, days=30)
    print(analytics_json)