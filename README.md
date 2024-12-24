# פלטפורמת ניתוח נתוני טרור

## תיאור הפרויקט
פרויקט זה נועד ליצור פלטפורמה לניתוח נתונים אנליטיים על אירועי טרור, המבוססים על נתונים היסטוריים ןעדכניים. הפלטפורמה מספקת ניתוחים סטטיסטיים, וויזואליזציה של נתונים כמפות.

## תכונות עיקריות
- **עיבוד נתונים**: ניקוי ועיבוד נתונים מ-CSV וממקורות נוספים.
- **ממשק API מבוסס Flask**: גישה לנתונים וניתוחים דרך קצה API.
- **הצגת מפות **: שימוש ב- Folium להצגת מפות וניתוחים גיאוגרפיים.
- **מיזוג נתונים**: שילוב נתונים ממספר מקורות.
- **שאיבת נתונים בזמן אמת**: שימוש ב-API חיצוניים לצורך עדכון נתונים.

## טכנולוגיות בשימוש
- **Pandas**, **cypher**: לניתוח ועיבוד נתונים.
- **Flask**: לבניית הממשק האינטרנטי.
- **Folium**: להצגת גרפים ומפות.
- **Neo4j**: לאחסון נתונים.

## הוראות הפעלה
1. התקן את כל התלויות הדרושות על ידי הרצת הפקודה:
   ```bash
   pip install -r requirements.txt