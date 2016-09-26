/**
 *
 * MapBuilder
 *
 * Copyright (c) 2015 by Udaya
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.mapbuilderfreq;

public class MsgData {

    public int MsgId;
    public float Longitude;
    public float Latitude;
    public String PlacePolygon;
    public String UserLocation;
    public float TagScore;    
    public float NormalizedScore;
    public float CombinedScore;

    public MsgData() {
    }

    public int getMsgId() {
        return MsgId;
    }

    public void setMsgId(int MsgId) {
        this.MsgId = MsgId;
    }

    public float getLongitude() {
        return Longitude;
    }

    public void setLongitude(float Longitude) {
        this.Longitude = Longitude;
    }

    public float getLatitude() {
        return Latitude;
    }

    public void setLatitude(float Latitude) {
        this.Latitude = Latitude;
    }

    public String getPlacePolygon() {
        return PlacePolygon;
    }

    public void setPlacePolygon(String PlacePolygon) {
        this.PlacePolygon = PlacePolygon;
    }

    public String getUserLocation() {
        return UserLocation;
    }

    public void setUserLocation(String UserLocation) {
        this.UserLocation = UserLocation;
    }

    public float getTagScore() {
        return TagScore;
    }

    public void setTagScore(float TagScore) {
        this.TagScore = TagScore;
    }

    public float getNormalizedScore() {
        return NormalizedScore;
    }

    public void setNormalizedScore(float NormalizedScore) {
        this.NormalizedScore = NormalizedScore;
    }

    public float getCombinedScore() {
        return CombinedScore;
    }

    public void setCombinedScore(float CombinedScore) {
        this.CombinedScore = CombinedScore;
    }

}
